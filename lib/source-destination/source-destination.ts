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

import { Transform as TransformStream } from 'stream';
import { EventEmitter } from 'events';
import { ReadResult, WriteResult } from 'file-disk';
import * as fileType from 'file-type';
import { getPartitions, GetPartitionsResult } from 'partitioninfo';
import { extname } from 'path';
import { XXHash3 } from 'xxhash-addon';

import {
	AlignedLockableBuffer,
	isAlignedLockableBuffer,
} from '../aligned-lockable-buffer';
import { CHUNK_SIZE, XXHASH_SEED } from '../constants';
import { ChecksumVerificationError, NotCapable } from '../errors';
import { BlocksWithChecksum, SparseReadable } from '../sparse-stream/shared';
import { SparseWritable } from '../sparse-stream/shared';
import { SparseFilterStream } from '../sparse-stream/sparse-filter-stream';
import { SparseReadStream } from '../sparse-stream/sparse-read-stream';
import { asCallback, fromCallback, streamToBuffer } from '../utils';

import { Metadata } from './metadata';
import {
	makeClassEmitProgressEvents,
	ProgressEvent,
	ProgressWritable,
} from './progress';
import { SourceSource } from './source-source';

class HashStream extends TransformStream {
	private _outEnc: string;
	private _hash: XXHash3;
	constructor(seed: number, outEnc: string | Buffer) {
		super();
		if (outEnc && typeof outEnc !== 'string' && !Buffer.isBuffer(outEnc)) {
			outEnc = 'buffer';
		}

		this._outEnc = outEnc as string;

		this._hash = new XXHash3(seed);
	}

	_transform(chunk: Buffer, _encoding: string, callback: () => void) {
		this._hash.update(chunk);
		callback();
	}
	_flush(callback: () => void) {
		this.push(this._hash.digest(this._outEnc));
		callback();
	}
}

export class CountingHashStream extends HashStream {
	public bytesWritten = 0;

	public async __transform(
		chunk: Buffer | AlignedLockableBuffer,
		encoding: string,
	): Promise<void> {
		const unlock = isAlignedLockableBuffer(chunk)
			? await chunk.rlock()
			: undefined;
		try {
			await fromCallback((callback: (error?: Error) => void) => {
				super._transform(chunk, encoding, callback);
			});
		} finally {
			unlock?.();
		}
		this.bytesWritten += chunk.length;
	}

	public _transform(
		chunk: Buffer | AlignedLockableBuffer,
		encoding: string,
		callback: (error?: Error) => void,
	) {
		asCallback(this.__transform(chunk, encoding), callback);
	}
}

export const ProgressHashStream = makeClassEmitProgressEvents(
	CountingHashStream,
	'bytesWritten',
	'bytesWritten',
);

export function createHasher() {
	const hasher = new ProgressHashStream(XXHASH_SEED, 'buffer');
	hasher.on('finish', async () => {
		const checksum = (await streamToBuffer(hasher)).toString('hex');
		hasher.emit('checksum', checksum);
	});
	return hasher;
}

export class SourceDestinationFs {
	// Adapts a SourceDestination to an fs like interface (so it can be used in udif for example)
	constructor(private source: SourceDestination) {}

	public open(
		_path: string,
		_options: any,
		callback: (error: Error | null, fd?: number) => void,
	) {
		callback(null, 1);
	}

	public close(_fd: number, callback: (error: Error | null) => void) {
		callback(null);
	}

	public fstat(
		_fd: number,
		callback: (error: Error | null, stats?: { size: number }) => void,
	) {
		this.source
			.getMetadata()
			.then((metadata) => {
				if (metadata.size === undefined) {
					callback(new Error('No size'));
					return;
				}
				callback(null, { size: metadata.size });
			})
			.catch(callback);
	}

	public read(
		_fd: number,
		buffer: Buffer,
		bufferOffset: number,
		length: number,
		sourceOffset: number,
		callback: (
			error: Error | null,
			bytesRead?: number,
			buffer?: Buffer,
		) => void,
	) {
		this.source
			.read(buffer, bufferOffset, length, sourceOffset)
			.then((res: ReadResult) => {
				callback(null, res.bytesRead, res.buffer);
			})
			.catch(callback);
	}

	// TODO: add write if it is needed
}

export abstract class Verifier extends EventEmitter {
	public progress: ProgressEvent = {
		bytes: 0,
		position: 0,
		speed: 0,
		averageSpeed: 0,
	};

	public abstract run(): Promise<void>;

	protected handleEventsAndPipe(
		stream: NodeJS.ReadableStream,
		meter: NodeJS.WritableStream,
	) {
		meter.on('progress', (progress: ProgressEvent) => {
			this.progress = progress;
			this.emit('progress', progress);
		});
		stream.on('end', this.emit.bind(this, 'end'));
		meter.on('finish', this.emit.bind(this, 'finish'));
		stream.once('error', () => {
			stream.unpipe(meter);
			meter.end();
		});
		stream.pipe(meter);
	}
}

export class StreamVerifier extends Verifier {
	constructor(
		private source: SourceDestination,
		private checksum: string,
		private size: number,
	) {
		super();
	}

	public async run(): Promise<void> {
		const stream = await this.source.createReadStream({
			end: this.size - 1,
			alignment: this.source.getAlignment(),
		});
		stream.on('error', this.emit.bind(this, 'error'));
		const hasher = createHasher();
		hasher.on('error', this.emit.bind(this, 'error'));
		hasher.on('checksum', (streamChecksum: string) => {
			if (streamChecksum !== this.checksum) {
				this.emit(
					'error',
					new ChecksumVerificationError(
						`Source and destination checksums do not match: ${this.checksum} !== ${streamChecksum}`,
						streamChecksum,
						this.checksum,
					),
				);
			}
		});
		this.handleEventsAndPipe(stream, hasher);
	}
}

export class SparseStreamVerifier extends Verifier {
	constructor(
		private source: SourceDestination,
		private blocks: BlocksWithChecksum[],
	) {
		super();
	}

	public async run(): Promise<void> {
		const alignment = this.source.getAlignment();
		let stream: SparseReadStream | SparseFilterStream;
		if (await this.source.canRead()) {
			stream = new SparseReadStream({
				source: this.source,
				blocks: this.blocks,
				chunkSize: CHUNK_SIZE,
				verify: true,
				generateChecksums: false,
				alignment,
				numBuffers: 2,
			});
			stream.on('error', this.emit.bind(this, 'error'));
		} else if (await this.source.canCreateReadStream()) {
			const originalStream = await this.source.createReadStream();
			originalStream.once('error', (error: Error) => {
				originalStream.unpipe(transform);
				this.emit('error', error);
			});
			const transform = new SparseFilterStream({
				blocks: this.blocks,
				verify: true,
				generateChecksums: false,
			});
			transform.once('error', (error: Error) => {
				originalStream.unpipe(transform);
				// @ts-ignore
				if (typeof originalStream.destroy === 'function') {
					// @ts-ignore
					originalStream.destroy();
				}
				this.emit('error', error);
			});
			originalStream.pipe(transform);
			stream = transform;
		} else {
			throw new NotCapable();
		}
		const meter = new ProgressWritable({ objectMode: true });
		this.handleEventsAndPipe(stream, meter);
	}
}

export interface CreateReadStreamOptions {
	emitProgress?: boolean;
	start?: number;
	end?: number;
	alignment?: number;
	numBuffers?: number;
}

export interface CreateSparseReadStreamOptions {
	generateChecksums?: boolean;
	alignment?: number;
	numBuffers?: number;
}

export class SourceDestination extends EventEmitter {
	public static readonly imageExtensions = [
		'img',
		'iso',
		'bin',
		'dsk',
		'hddimg',
		'raw',
		'dmg',
		'sdcard',
		'rpi-sdimg',
		'wic',
	];
	public static readonly mimetype?: string;
	private static mimetypes = new Map<string, typeof SourceSource>();

	private metadata: Metadata;
	private isOpen = false;

	public static register(Cls: typeof SourceSource) {
		if (Cls.mimetype !== undefined) {
			SourceDestination.mimetypes.set(Cls.mimetype, Cls);
		}
	}

	public getAlignment(): number | undefined {
		return undefined;
	}

	public async canRead(): Promise<boolean> {
		return false;
	}

	public async canWrite(): Promise<boolean> {
		return false;
	}

	public async canCreateReadStream(): Promise<boolean> {
		return false;
	}

	public async canCreateSparseReadStream(): Promise<boolean> {
		return false;
	}

	public async canCreateWriteStream(): Promise<boolean> {
		return false;
	}

	public async canCreateSparseWriteStream(): Promise<boolean> {
		return false;
	}

	public async getMetadata(): Promise<Metadata> {
		if (this.metadata === undefined) {
			this.metadata = await this._getMetadata();
		}
		return this.metadata;
	}

	protected async _getMetadata(): Promise<Metadata> {
		return {};
	}

	public async read(
		_buffer: Buffer,
		_bufferOffset: number,
		_length: number,
		_sourceOffset: number,
	): Promise<ReadResult> {
		throw new NotCapable();
	}

	public async write(
		_buffer: Buffer,
		_bufferOffset: number,
		_length: number,
		_fileOffset: number,
	): Promise<WriteResult> {
		throw new NotCapable();
	}

	public async createReadStream(
		_options: CreateReadStreamOptions = {},
	): Promise<NodeJS.ReadableStream> {
		throw new NotCapable();
	}

	public async createSparseReadStream(
		_options: CreateSparseReadStreamOptions = {},
	): Promise<SparseReadable> {
		throw new NotCapable();
	}

	public async getBlocks(): Promise<BlocksWithChecksum[]> {
		throw new NotCapable();
	}

	public async createWriteStream(
		_options: { highWaterMark?: number } = {},
	): Promise<NodeJS.WritableStream> {
		throw new NotCapable();
	}

	public async createSparseWriteStream(
		_options: { highWaterMark?: number } = {},
	): Promise<SparseWritable> {
		throw new NotCapable();
	}

	public async open(): Promise<void> {
		if (!this.isOpen) {
			await this._open();
			this.isOpen = true;
		}
	}

	public async close(): Promise<void> {
		if (this.isOpen) {
			await this._close();
			this.isOpen = false;
		}
	}

	protected async _open(): Promise<void> {
		// noop
	}

	protected async _close(): Promise<void> {
		// noop
	}

	public createVerifier(
		checksumOrBlocks: string | BlocksWithChecksum[],
		size?: number,
	): Verifier {
		if (Array.isArray(checksumOrBlocks)) {
			for (const block of checksumOrBlocks) {
				if (block.checksumType === undefined || block.checksum === undefined) {
					throw new Error(
						'Block is missing checksum or checksumType attributes, can not create verifier',
					);
				}
			}
			return new SparseStreamVerifier(this, checksumOrBlocks);
		} else {
			if (size === undefined) {
				throw new Error(
					'A size argument is required for creating a stream checksum verifier',
				);
			}
			return new StreamVerifier(this, checksumOrBlocks, size);
		}
	}

	private async getMimeTypeFromName(): Promise<string | undefined> {
		const metadata = await this.getMetadata();
		if (metadata.name === undefined) {
			return;
		}
		const extension = extname(metadata.name).toLowerCase();
		if (extension === '.dmg') {
			return 'application/x-apple-diskimage';
		}
	}

	private async getMimeTypeFromContent(): Promise<string | undefined> {
		let stream: NodeJS.ReadableStream;
		try {
			stream = await this.createReadStream({
				end: 263, // TODO: constant
				alignment: this.getAlignment(),
			});
		} catch (error) {
			if (error instanceof NotCapable) {
				return;
			}
			throw error;
		}
		const ft = fileType(await streamToBuffer(stream));
		if (ft !== null) {
			return ft.mime;
		}
	}

	private async getInnerSourceHelper(mimetype?: string) {
		if (mimetype === undefined) {
			return this;
		}
		const Cls = SourceDestination.mimetypes.get(mimetype);
		if (Cls === undefined) {
			return this;
		}
		if (Cls.requiresRandomReadableSource && !(await this.canRead())) {
			throw new NotCapable(
				`Can not read a ${Cls.name} from a ${this.constructor.name}.`,
			);
		}
		const innerSource = new Cls(this);
		return innerSource.getInnerSource();
	}

	public async getInnerSource(): Promise<SourceDestination> {
		await this.open();
		const metadata = await this.getMetadata();
		if (metadata.isEtch === true) {
			return this;
		}
		let mimetype = await this.getMimeTypeFromName();
		if (mimetype !== undefined) {
			try {
				return await this.getInnerSourceHelper(mimetype);
			} catch (error) {
				if (error instanceof NotCapable) {
					throw error;
				}
				// File extension may be wrong, try content.
			}
		}
		mimetype = await this.getMimeTypeFromContent();
		return this.getInnerSourceHelper(mimetype);
	}

	public async getPartitionTable(): Promise<GetPartitionsResult | undefined> {
		const stream = await this.createReadStream({
			end: 34 * 512, // GPT partition table size
			alignment: this.getAlignment(),
		});
		const buffer = await streamToBuffer(stream);
		try {
			return await getPartitions(buffer, { getLogical: false });
		} catch {
			// no partitions
		}
	}
}
