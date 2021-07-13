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

import { promisify } from 'util';
import { spawn } from 'child_process';
import { EventEmitter } from 'events';
import { ReadResult, WriteResult } from 'file-disk';
import { platform } from 'os';
import { PassThrough } from 'stream';

import { PROGRESS_EMISSION_INTERVAL } from '../constants';
import { VerificationError } from '../errors';
import { BlocksWithChecksum, SparseReadable } from '../sparse-stream/shared';
import { SparseWritable } from '../sparse-stream/shared';
import { every, difference, minBy } from '../utils';
import { BlockDevice } from './block-device';
import { ProgressEvent } from './progress';
import {
	CreateReadStreamOptions,
	CreateSparseReadStreamOptions,
	SourceDestination,
	Verifier,
} from './source-destination';

const START_SHELLHWDETECTION_DELAY = 2000;

function isntNull<T>(x: T | null): x is Exclude<T, null> {
	return x !== null;
}

export class MultiDestinationError extends Error {
	constructor(public error: Error, public destination: SourceDestination) {
		super();
	}
}

export class MultiDestinationVerifier extends Verifier {
	private verifiers: Set<Verifier> = new Set();
	private timer: NodeJS.Timer;

	constructor(
		source: MultiDestination,
		checksumOrBlocks: string | BlocksWithChecksum[],
		size?: number,
	) {
		super();
		for (const dest of source.activeDestinations) {
			const verifier = dest.createVerifier(checksumOrBlocks, size);
			verifier.on('error', (error: Error) => {
				this.oneVerifierFinished(verifier);
				source.destinationError(dest, error, this);
			});
			verifier.on('finish', () => {
				this.oneVerifierFinished(verifier);
			});
			this.verifiers.add(verifier);
		}
	}

	private oneVerifierFinished(verifier: Verifier) {
		if (!this.verifiers.has(verifier)) {
			return;
		}
		if (this.verifiers.size === 1) {
			clearInterval(this.timer);
			this.emitProgress();
			this.emit('finish');
		}
		this.verifiers.delete(verifier);
	}

	private emitProgress() {
		const verifier = minBy(this.verifiers, (v: Verifier) => {
			return v.progress.bytes;
		});
		if (verifier !== undefined) {
			this.emit('progress', verifier.progress);
		}
	}

	public async run(): Promise<void> {
		if (this.verifiers.size === 0) {
			this.emit('finish');
			return;
		}
		this.timer = setInterval(
			this.emitProgress.bind(this),
			PROGRESS_EMISSION_INTERVAL,
		);
		for (const verifier of this.verifiers) {
			verifier.run();
		}
	}
}

export class MultiDestination extends SourceDestination {
	// MultiDestination does not emit 'error' events, only 'fail' events wrapping the original error in a MultiDestinationError
	public readonly destinations: Set<SourceDestination> = new Set();
	public readonly erroredDestinations: Set<SourceDestination> = new Set();

	constructor(destinations: SourceDestination[]) {
		super();
		if (destinations.length === 0) {
			throw new Error('At least one destination is required');
		}
		for (const destination of destinations) {
			this.destinations.add(destination);
		}
	}

	public getAlignment(): number | undefined {
		let max;
		for (const destination of this.destinations.values()) {
			if (
				destination instanceof BlockDevice &&
				(max === undefined || destination.alignment > max)
			) {
				max = destination.alignment;
			}
		}
		return max;
	}

	public destinationError(
		destination: SourceDestination,
		error: Error,
		stream?: EventEmitter,
	) {
		// If a stream is provided, emit the 'fail' event on it, instead of this instance.
		if (!(error instanceof VerificationError)) {
			// Verification errors aren't fatal
			this.erroredDestinations.add(destination);
		}
		error = new MultiDestinationError(error, destination);
		// Don't emit 'error' events as it would unpipe the source from the stream
		if (stream !== undefined) {
			stream.emit('fail', error);
		} else {
			this.emit('fail', error);
		}
	}

	get activeDestinations(): Set<SourceDestination> {
		return difference(this.destinations, this.erroredDestinations);
	}

	private async can(
		methodName:
			| 'canRead'
			| 'canWrite'
			| 'canCreateReadStream'
			| 'canCreateSparseReadStream'
			| 'canCreateWriteStream'
			| 'canCreateSparseWriteStream',
	) {
		return every(
			await Promise.all(
				Array.from(this.activeDestinations).map((destination) =>
					destination[methodName](),
				),
			),
		);
	}

	public canRead(): Promise<boolean> {
		return this.can('canRead');
	}

	public canWrite(): Promise<boolean> {
		return this.can('canWrite');
	}

	public canCreateReadStream(): Promise<boolean> {
		return this.can('canCreateReadStream');
	}

	public canCreateSparseReadStream(): Promise<boolean> {
		return this.can('canCreateSparseReadStream');
	}

	public canCreateWriteStream(): Promise<boolean> {
		return this.can('canCreateWriteStream');
	}

	public canCreateSparseWriteStream(): Promise<boolean> {
		return this.can('canCreateSparseWriteStream');
	}

	public read(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		sourceOffset: number,
	): Promise<ReadResult> {
		// Reads from the first destination (supposing all destinations contain the same data)
		return Array.from(this.activeDestinations)[0].read(
			buffer,
			bufferOffset,
			length,
			sourceOffset,
		);
	}

	public async write(
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		fileOffset: number,
	): Promise<WriteResult> {
		const results = await Promise.all(
			Array.from(this.activeDestinations).map((destination) =>
				destination.write(buffer, bufferOffset, length, fileOffset),
			),
		);
		// Returns the first WriteResult (they should be all the same)
		return results[0];
		// TODO: handle errors so one destination can fail
	}

	public createReadStream(
		options: CreateReadStreamOptions,
	): Promise<NodeJS.ReadableStream> {
		// TODO: raise an error or a warning here
		return Array.from(this.activeDestinations)[0].createReadStream(options);
	}

	public createSparseReadStream(
		options: CreateSparseReadStreamOptions,
	): Promise<SparseReadable> {
		// TODO: raise an error or a warning here
		return Array.from(this.activeDestinations)[0].createSparseReadStream(
			options,
		);
	}

	private async createStream(
		methodName: 'createWriteStream',
		...args: Parameters<SourceDestination['createWriteStream']>
	): Promise<NodeJS.WritableStream>;

	private async createStream(
		methodName: 'createSparseWriteStream',
		...args: Parameters<SourceDestination['createSparseWriteStream']>
	): Promise<SparseWritable>;

	private async createStream(
		methodName: 'createWriteStream' | 'createSparseWriteStream',
		...args:
			| Parameters<SourceDestination['createWriteStream']>
			| Parameters<SourceDestination['createSparseWriteStream']>
	) {
		const passthrough = new PassThrough({
			objectMode: methodName === 'createSparseWriteStream',
		});
		// all streams listen to end events, +1 because we'll listen too
		const listeners = this.activeDestinations.size + 1;
		if (listeners > EventEmitter.defaultMaxListeners) {
			passthrough.setMaxListeners(listeners);
		}
		const progresses: Map<
			NodeJS.WritableStream,
			ProgressEvent | null
		> = new Map();
		let interval: NodeJS.Timer;

		function oneStreamFinished(stream: NodeJS.WritableStream) {
			if (!progresses.has(stream)) {
				return;
			}
			if (progresses.size === 1) {
				clearInterval(interval);
				emitProgress(); // Just to be sure we emitted the last state
				passthrough.emit('done');
			}
			progresses.delete(stream);
		}

		function emitProgress() {
			// TODO: avoid Array.from
			const leastAdvancedProgress = minBy(
				Array.from(progresses.values()).filter(isntNull),
				(p) => p.position,
			);
			if (leastAdvancedProgress !== undefined) {
				passthrough.emit('progress', leastAdvancedProgress);
			}
		}

		await Promise.all(
			Array.from(this.activeDestinations).map(async (destination) => {
				const stream = await destination[methodName](...args);
				progresses.set(stream, null);
				stream.on('progress', (progressEvent: ProgressEvent) => {
					progresses.set(stream, progressEvent);
					if (interval === undefined) {
						interval = setInterval(emitProgress, PROGRESS_EMISSION_INTERVAL);
					}
				});
				stream.on('error', (error: Error) => {
					this.destinationError(destination, error, passthrough);
					oneStreamFinished(stream);
				});
				stream.on('finish', oneStreamFinished.bind(null, stream));
				passthrough.pipe(stream);
			}),
		);

		passthrough.on('pipe', () => {
			// Handle the special case where we have zero destination streams
			if (this.activeDestinations.size === 0) {
				passthrough.emit('done');
			}
		});
		return passthrough;
	}

	public createWriteStream(
		...args: Parameters<SourceDestination['createWriteStream']>
	): Promise<NodeJS.WritableStream> {
		return this.createStream('createWriteStream', ...args);
	}

	public createSparseWriteStream(
		...args: Parameters<SourceDestination['createSparseWriteStream']>
	): Promise<SparseWritable> {
		return this.createStream('createSparseWriteStream', ...args);
	}

	public createVerifier(
		checksumOrBlocks: string | BlocksWithChecksum[],
		size?: number,
	): Verifier {
		return new MultiDestinationVerifier(this, checksumOrBlocks, size);
	}

	protected async _open(): Promise<void> {
		await Promise.all(
			Array.from(this.destinations).map(async (destination) => {
				try {
					await destination.open();
				} catch (error) {
					this.destinationError(destination, error);
				}
			}),
		);
	}

	protected async _close(): Promise<void> {
		const hasPhysicalDrive = Array.from(this.destinations)
			.some((destination: BlockDevice) => {
				return destination.path?.toLowerCase().startsWith('\\\\.\\physicaldrive');
			});
		const restartSHWD = platform() === 'win32' && hasPhysicalDrive;
		// restart ShellHWDetection to prevent "Format Disk" warning popups
		if (restartSHWD) {
			// We don't really care if this fails, as it's non-essential
			await new Promise((res) => {
				const cp = spawn('sc', ['stop', 'ShellHWDetection']);
				cp.on('exit', res);
			})
		}

		await Promise.all(
			Array.from(this.destinations).map(async (destination) => {
				try {
					await destination.close();
				} catch (error) {
					this.destinationError(destination, error);
				}
			}),
		);

		if (restartSHWD) {
			promisify(setTimeout)(START_SHELLHWDETECTION_DELAY)
				.then(() => spawn('sc', ['start', 'ShellHWDetection']));
		}
	}
}
