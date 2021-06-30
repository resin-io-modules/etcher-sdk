/*
 * Copyright 2018 balena.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { promises as fs } from 'fs';
import { extname } from 'path';

import { BlockReadStream } from './block-read-stream';
import { BlockTransformStream } from './block-transform-stream';
import { CHUNK_SIZE } from './constants';
import { getRootStream } from './source-destination/compressed-source';
import {
	ConfiguredSource,
	ConfigureFunction,
} from './source-destination/configured-source/configured-source';
import { File } from './source-destination/file';
import { Metadata } from './source-destination/metadata';
import {
	MultiDestination,
	MultiDestinationError,
} from './source-destination/multi-destination';
import { ProgressEvent } from './source-destination/progress';
import {
	CountingHashStream,
	createHasher,
	SourceDestination,
	Verifier,
} from './source-destination/source-destination';
import { freeSpace, tmpFile } from './tmp';

export type WriteStep = 'decompressing' | 'flashing' | 'verifying' | 'finished';

export const DECOMPRESSED_IMAGE_PREFIX = 'decompressed-image-';

interface MultiDestinationState {
	active: number;
	failed: number;
	type: WriteStep;
	size?: number;
	compressedSize?: number;
	blockmappedSize?: number;
	sparse?: boolean;
	rootStreamPosition?: number;
	rootStreamSpeed?: number;
	rootStreamAverageSpeed?: number;
	bytesWritten?: number;
}

export interface MultiDestinationProgress extends MultiDestinationState {
	bytes: number;
	position: number;
	speed: number;
	averageSpeed: number;
	percentage?: number;
	eta?: number;
}

export type OnFailFunction = (
	destination: SourceDestination,
	error: Error,
) => void;

export type OnProgressFunction = (progress: MultiDestinationProgress) => void;

export interface PipeSourceToDestinationsResult {
	failures: Map<SourceDestination, Error>;
	bytesWritten: number;
	sourceMetadata: Metadata;
}

function getEta(
	current: number,
	total: number,
	speed: number,
): number | undefined {
	return speed === 0 ? undefined : (total - current) / speed;
}

function isWorthDecompressing(filename = '') {
	return [
		'.img',
		'.bin',
		'.hddimg',
		'.raw',
		'.sdcard',
		'.rpi-sdimg',
		'.wic',
	].includes(extname(filename));
}

function defaultEnoughSpaceForDecompression(free: number, imageSize?: number) {
	return imageSize === undefined
		? false
		: imageSize < Math.min(free / 2, 5 * 1024 ** 3);
}

export async function decompressThenFlash({
	source,
	destinations,
	onFail,
	onProgress,
	verify = false,
	numBuffers = 16,
	decompressFirst = false,
	trim = false,
	configure,
	enoughSpaceForDecompression = defaultEnoughSpaceForDecompression,
	asItIs = false,
}: {
	source: SourceDestination;
	destinations: SourceDestination[];
	onFail: OnFailFunction;
	onProgress: OnProgressFunction;
	verify?: boolean;
	numBuffers?: number;
	decompressFirst?: boolean;
	trim?: boolean;
	configure?: ConfigureFunction;
	enoughSpaceForDecompression?: (free: number, imageSize?: number) => boolean;
	asItIs?: boolean;
}): Promise<PipeSourceToDestinationsResult> {
	await source.open();
	if (!asItIs) {
		source = await source.getInnerSource();
	}
	const sourceMetadata = await source.getMetadata();
	const enoughDiskSpaceAvailable = enoughSpaceForDecompression(
		await freeSpace(),
		sourceMetadata.size,
	);
	let decompressedFilePath: string | undefined;
	try {
		if (
			decompressFirst &&
			sourceMetadata.isCompressed &&
			isWorthDecompressing(sourceMetadata.name) &&
			enoughDiskSpaceAvailable
		) {
			({ path: decompressedFilePath } = await tmpFile({
				keepOpen: false,
				prefix: DECOMPRESSED_IMAGE_PREFIX,
			}));
			const decompressedSource = new File({
				path: decompressedFilePath,
				write: true,
			});
			await decompressedSource.open();
			const inputStream = await source.createReadStream();
			const outputStream = await decompressedSource.createWriteStream();
			await new Promise((resolve, reject) => {
				outputStream.on('done', resolve);
				outputStream.on('error', reject);
				inputStream.on('error', reject);
				const state = {
					active: 0,
					failed: 0,
					type: 'decompressing' as const,
				};
				const [$onProgress, onRootStreamProgress] = createCompleteOnProgress(
					onProgress,
					sourceMetadata,
					state,
					false,
				);
				getRootStream(inputStream).on('progress', onRootStreamProgress);
				outputStream.on('progress', $onProgress);
				inputStream.pipe(outputStream);
			});
			source = decompressedSource;
		}
		if (
			(trim || configure !== undefined) &&
			!(source instanceof ConfiguredSource)
		) {
			if (!(await source.canRead())) {
				console.warn(
					"Can't configure or trim a source that is not randomly readable, skipping",
				);
			} else {
				source = new ConfiguredSource({
					source,
					shouldTrimPartitions: trim,
					createStreamFromDisk: decompressFirst || !sourceMetadata.isCompressed,
					configure,
				});
				await source.open();
			}
		}
		return await pipeSourceToDestinations({
			source,
			destinations,
			onFail,
			onProgress,
			verify,
			numBuffers,
		});
	} finally {
		if (decompressedFilePath) {
			await fs.unlink(decompressedFilePath);
		}
	}
}

function createCompleteOnProgress(
	onProgress: (p: MultiDestinationProgress) => void,
	sourceMetadata: Metadata,
	state: MultiDestinationState,
	sparse: boolean,
) {
	function $onProgress(progress: ProgressEvent) {
		// sourceMetadata will be updated by pipeRegularSourceToDestination
		if (sourceMetadata.size !== undefined && state.size === undefined) {
			state.size = sourceMetadata.size;
		}
		let size: number | undefined;
		let percentage: number | undefined;
		let eta: number | undefined;
		if (sparse) {
			size = state.blockmappedSize;
			state.bytesWritten = progress.bytes;
		} else {
			size = state.size;
			state.bytesWritten = progress.position;
		}
		if (
			size !== undefined &&
			state.bytesWritten !== undefined &&
			state.bytesWritten <= size
		) {
			percentage = (state.bytesWritten / size) * 100;
			eta = getEta(state.bytesWritten, size, progress.speed);
		} else if (
			state.rootStreamSpeed !== undefined &&
			state.rootStreamPosition !== undefined &&
			state.compressedSize !== undefined
		) {
			percentage = (state.rootStreamPosition / state.compressedSize) * 100;
			eta = getEta(
				state.rootStreamPosition,
				state.compressedSize,
				state.rootStreamSpeed,
			);
		}
		const result: MultiDestinationProgress = {
			...progress,
			...state,
			percentage,
			eta,
		};
		onProgress(result);
	}

	function onRootStreamProgress(progress: ProgressEvent) {
		state.rootStreamPosition = progress.position;
		state.rootStreamSpeed = progress.speed;
		state.rootStreamAverageSpeed = progress.averageSpeed;
	}

	return [$onProgress, onRootStreamProgress];
}

// This function is the most common use case of the SDK.
// Added it here to avoid duplicating it in other projects.
export async function pipeSourceToDestinations({
	source,
	destinations,
	onFail,
	onProgress,
	verify = false,
	numBuffers = 16,
}: {
	source: SourceDestination;
	destinations: SourceDestination[];
	onFail: OnFailFunction;
	onProgress: OnProgressFunction;
	verify?: boolean;
	numBuffers?: number;
}): Promise<PipeSourceToDestinationsResult> {
	if (numBuffers < 2) {
		numBuffers = 2;
	}
	const destination = new MultiDestination(destinations);
	const failures: Map<SourceDestination, Error> = new Map();

	const state: MultiDestinationState = {
		active: destination.destinations.size,
		failed: 0,
		type: 'flashing',
	};

	destination.on('fail', _onFail);
	await Promise.all([source.open(), destination.open()]);

	const [sourceMetadata, sparseSource, sparseDestination] = await Promise.all([
		source.getMetadata(),
		source.canCreateSparseReadStream(),
		destination.canCreateSparseWriteStream(),
	]);
	const sparse = sparseSource && sparseDestination;

	state.sparse = sparse;
	state.size = sourceMetadata.size;
	state.compressedSize = sourceMetadata.compressedSize;
	state.blockmappedSize = sourceMetadata.blockmappedSize;

	function updateState(step?: WriteStep) {
		if (step !== undefined) {
			state.type = step;
		}
		state.failed = failures.size;
		state.active = destination.destinations.size - state.failed;
	}

	function _onFail(error: MultiDestinationError) {
		failures.set(error.destination, error.error);
		updateState();
		onFail(error.destination, error.error);
	}

	const [$onProgress, onRootStreamProgress] = createCompleteOnProgress(
		onProgress,
		sourceMetadata,
		state,
		sparse,
	);

	if (sparse) {
		await pipeSparseSourceToDestination(
			source,
			destination,
			verify,
			numBuffers,
			updateState,
			_onFail,
			$onProgress,
			onRootStreamProgress,
		);
	} else {
		await pipeRegularSourceToDestination(
			source,
			sourceMetadata,
			destination,
			verify,
			numBuffers,
			updateState,
			_onFail,
			$onProgress,
			onRootStreamProgress,
		);
	}
	updateState('finished');
	await Promise.all([source.close(), destination.close()]);
	return { sourceMetadata, failures, bytesWritten: state.bytesWritten || 0 };
}

function notUndefined<T>(x: T | undefined): x is T {
	return x !== undefined;
}

/** TODO: Why are we getting the highest alignment? Shouldn't we be using the lowest alignment? */
function getAlignment(...devices: SourceDestination[]): number | undefined {
	const alignments = devices.map((d) => d.getAlignment()).filter(notUndefined);
	if (alignments.length) {
		return Math.max(...alignments);
	}
}

async function pipeRegularSourceToDestination(
	source: SourceDestination,
	sourceMetadata: Metadata,
	destination: MultiDestination,
	verify: boolean,
	numBuffers: number,
	updateState: (state?: WriteStep) => void,
	onFail: (error: MultiDestinationError) => void,
	onProgress: (progress: ProgressEvent) => void,
	onRootStreamProgress: (progress: ProgressEvent) => void,
): Promise<void> {
	let lastPosition = 0;
	const emitSourceProgress =
		sourceMetadata.size === undefined || sourceMetadata.isSizeEstimated;
	const alignment = getAlignment(source, destination);
	/** TODO: Why is the highWaterMark affected by alignment? */
	const highWaterMark = alignment === undefined ? undefined : numBuffers - 1;
	const [sourceStream, destinationStream] = await Promise.all([
		source.createReadStream({
			emitProgress: emitSourceProgress,
			alignment,
			numBuffers,
		}),
		destination.createWriteStream({ highWaterMark }),
	]);
	getRootStream(sourceStream).on('progress', (progress: ProgressEvent) => {
		onRootStreamProgress(progress);
	});
	const checksum = await new Promise(
		(
			resolve: (checksum: string | undefined) => void,
			reject: (error: Error) => void,
		) => {
			let result: string;
			let done = false;
			let hasher: CountingHashStream;
			function maybeDone(maybeChecksum?: string) {
				if (maybeChecksum !== undefined) {
					result = maybeChecksum;
				} else {
					done = true;
				}
				if (
					done &&
					(!verify ||
						destination.activeDestinations.size === 0 ||
						result !== undefined)
				) {
					if (hasher !== undefined) {
						sourceStream.unpipe(hasher);
						hasher.end();
					}
					resolve(result);
				}
			}
			sourceStream.once('error', reject);
			destinationStream.on('fail', onFail); // This is emitted by MultiDestination when one of its destinations fails
			destinationStream.once('error', reject);
			if (verify) {
				hasher = createHasher();
				hasher.once('checksum', maybeDone);
				sourceStream.pipe(hasher);
			}
			destinationStream.once('done', maybeDone);
			destinationStream.on('progress', (progress: ProgressEvent) => {
				lastPosition = progress.position;
				onProgress(progress);
			});
			if (
				alignment !== undefined &&
				!(
					sourceStream instanceof BlockReadStream ||
					sourceStream instanceof BlockTransformStream
				)
			) {
				// The destination needs data to be aligned and it isn't.
				// Pass it through a BlockTransformStream to align it.
				sourceStream
					.pipe(
						new BlockTransformStream({
							chunkSize: CHUNK_SIZE,
							alignment,
							numBuffers,
						}),
					)
					.pipe(destinationStream);
			} else {
				sourceStream.pipe(destinationStream);
			}
		},
	);
	if (
		sourceMetadata.size === undefined ||
		sourceMetadata.isSizeEstimated === true
	) {
		sourceMetadata.size = lastPosition;
		sourceMetadata.isSizeEstimated = false;
	}
	if (verify && checksum) {
		updateState('verifying');
		const verifier = destination.createVerifier(checksum, lastPosition);
		await runVerifier(verifier, onFail, onProgress);
	}
}

async function pipeSparseSourceToDestination(
	source: SourceDestination,
	destination: MultiDestination,
	verify: boolean,
	numBuffers: number,
	updateState: (state?: WriteStep) => void,
	onFail: (error: MultiDestinationError) => void,
	onProgress: (progress: ProgressEvent) => void,
	onRootStreamProgress: (progress: ProgressEvent) => void,
): Promise<void> {
	const alignment = getAlignment(source, destination);
	const highWaterMark = alignment === undefined ? undefined : numBuffers - 1;
	const [sourceStream, destinationStream] = await Promise.all([
		source.createSparseReadStream({
			generateChecksums: verify,
			alignment,
			numBuffers,
		}),
		destination.createSparseWriteStream({ highWaterMark }),
	]);
	getRootStream(sourceStream).on('progress', (progress: ProgressEvent) => {
		onRootStreamProgress(progress);
	});
	await new Promise<void>(
		(resolve: () => void, reject: (error: Error) => void) => {
			sourceStream.once('error', reject);
			destinationStream.once('error', reject);
			destinationStream.once('done', resolve);
			destinationStream.on('fail', onFail); // This is emitted by MultiDestination when one of its destinations fails
			destinationStream.on('progress', onProgress);
			sourceStream.pipe(destinationStream);
		},
	);
	if (verify) {
		updateState('verifying');
		const verifier = destination.createVerifier(sourceStream.blocks);
		await runVerifier(verifier, onFail, onProgress);
	}
}

async function runVerifier(
	verifier: Verifier,
	onFail: (error: MultiDestinationError) => void,
	onProgress: (progress: ProgressEvent) => void,
) {
	await new Promise((resolve, reject) => {
		verifier.once('error', reject);
		verifier.once('finish', resolve);
		verifier.on('fail', onFail);
		verifier.on('progress', onProgress);
		verifier.run();
	});
}
