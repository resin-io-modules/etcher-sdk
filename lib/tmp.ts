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

import * as checkDiskSpace from 'check-disk-space';
import { randomBytes } from 'crypto';
import { Dirent, promises as fs } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

const TMP_RANDOM_BYTES = 6;
const TMP_DIR = join(tmpdir(), 'etcher');
const TRIES = 5;

function randomFilePath(prefix: string, postfix: string): string {
	return join(
		TMP_DIR,
		`${prefix}${randomBytes(TMP_RANDOM_BYTES).toString('hex')}${postfix}`,
	);
}

export interface TmpFileResult {
	path: string;
	fileHandle?: fs.FileHandle;
}

export async function cleanupTmpFiles(
	olderThan = Date.now(),
	prefix = '',
): Promise<void> {
	let dirents: Dirent[] = [];
	try {
		dirents = await fs.readdir(TMP_DIR, { withFileTypes: true });
	} catch {
		return;
	}
	for (const dirent of dirents) {
		if (dirent.isFile()) {
			if (dirent.name.startsWith(prefix)) {
				const filename = join(TMP_DIR, dirent.name);
				try {
					const stats = await fs.stat(filename);
					if (stats.ctime.getTime() <= olderThan) {
						await fs.unlink(filename);
					}
				} catch {
					// noop
				}
			}
		}
	}
}

async function createTmpRoot(): Promise<void> {
	try {
		await fs.mkdir(TMP_DIR, { recursive: true });
	} catch (error) {
		if (error.code === 'EEXIST' && !(await fs.stat(TMP_DIR)).isDirectory()) {
			await fs.unlink(TMP_DIR);
			await fs.mkdir(TMP_DIR, { recursive: true });
		}
	}
}

export interface TmpFileOptions {
	keepOpen?: boolean;
	prefix?: string;
	postfix?: string;
}

export async function tmpFile({
	keepOpen = true,
	prefix = 'tmp-',
	postfix = '.tmp',
}: TmpFileOptions): Promise<TmpFileResult> {
	await createTmpRoot();
	let fileHandle: fs.FileHandle | undefined;
	let path: string;
	let ok = false;
	for (let i = 0; i < TRIES; i++) {
		path = randomFilePath(prefix, postfix);
		try {
			fileHandle = await fs.open(path, 'wx+');
			ok = true;
			break;
		} catch (error) {
			if (error.code !== 'EEXIST') {
				throw error;
			}
		}
	}
	if (!ok) {
		throw new Error(
			`Could not generate a temporary filename in ${TRIES} tries`,
		);
	}
	if (!keepOpen && fileHandle !== undefined) {
		await fileHandle.close();
		fileHandle = undefined;
	}
	return { fileHandle, path: path! };
}

export async function withTmpFile<T>(
	options: TmpFileOptions,
	fn: (result: TmpFileResult) => Promise<T>,
): Promise<T> {
	const result = await tmpFile(options);
	try {
		return await fn(result);
	} finally {
		if (options.keepOpen && result.fileHandle !== undefined) {
			await result.fileHandle.close();
		}
		try {
			await fs.unlink(result.path);
		} catch (error) {
			// The file might already have been deleted by cleanupTmpFiles
			if (error.code !== 'ENOENT') {
				// tslint:disable-next-line:no-unsafe-finally
				throw error;
			}
		}
	}
}

export async function freeSpace() {
	try {
		return (await checkDiskSpace(TMP_DIR)).free;
	} catch (error) {
		console.warn(`Could not check free disk space in "${TMP_DIR}": ${error}`);
		return 0;
	}
}
