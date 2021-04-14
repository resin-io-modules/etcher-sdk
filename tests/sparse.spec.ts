/*
 * Copyright 2019 balena.io
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

import * as assert from 'assert';
import { join } from 'path';
import { stub } from 'sinon';

import { sourceDestination } from '../lib';
import { BlocksVerificationError } from '../lib/errors';
import { ChecksumType } from '../lib/sparse-stream/shared';
import { withTmpFile } from '../lib/tmp';
import { DEFAULT_IMAGE_TESTS_TIMEOUT } from './tester';

const DMG_PATH = join(__dirname, 'data', 'dmg', 'gpt-in-dmg.dmg');
const DISK_PATH = join(__dirname, 'data', 'images', 'disk.img');
const ZIP_PATH = join(
	__dirname,
	'data',
	'metadata',
	'zip',
	'etcher-test-with-bmap.zip',
);

describe('sparse streams', function () {
	this.timeout(DEFAULT_IMAGE_TESTS_TIMEOUT);

	it('dmgs, sparse streams and verifiers', async () => {
		const source = new sourceDestination.File({ path: DMG_PATH });
		const innerSource = await source.getInnerSource();
		assert(innerSource instanceof sourceDestination.DmgSource);

		const sourceSparseStream = await innerSource.createSparseReadStream();
		assert(sourceSparseStream.blocks.length === 1);
		assert(sourceSparseStream.blocks[0].checksum === '4c754335');
		assert(sourceSparseStream.blocks[0].blocks.length === 1);

		// Create a temporary destination file:
		await withTmpFile(
			{ keepOpen: false },
			async ({ path }: { path: string }) => {
				const destination = new sourceDestination.File({ path, write: true });
				await destination.open();
				// Test sparse write stream
				const destinationStream = await destination.createSparseWriteStream();
				await new Promise((resolve, reject) => {
					sourceSparseStream.on('error', reject);
					destinationStream.on('error', reject);
					destinationStream.on('done', resolve);
					sourceSparseStream.pipe(destinationStream);
				});

				// Test sparse verifier for randomly readable destination
				// (This tests SparseReadStream)
				const verifier = destination.createVerifier(sourceSparseStream.blocks);
				await new Promise((resolve, reject) => {
					verifier.on('error', reject);
					verifier.on('finish', resolve);
					verifier.run();
				});

				// Test sparse verifier for non randomly readable destination
				// (This tests SparseFilterStream)
				const canReadStub = stub(destination, 'canRead');
				canReadStub.resolves(false);
				const verifier2 = destination.createVerifier(sourceSparseStream.blocks);
				await new Promise((resolve, reject) => {
					verifier2.on('error', reject);
					verifier2.on('finish', resolve);
					verifier2.run();
				});
				canReadStub.restore();

				// Test sparse verifier with wrong checksum
				const wrongBlocks = JSON.parse(
					JSON.stringify(sourceSparseStream.blocks),
				);
				wrongBlocks[0].checksum = 'wrong';
				const brokenVerifier = destination.createVerifier(wrongBlocks);
				const verifierError: Error = await new Promise((resolve, reject) => {
					brokenVerifier.on('finish', () => {
						reject(new Error('There should have been a checksum mismatch'));
					});
					brokenVerifier.on('error', (error: Error) => {
						resolve(error);
					});
					brokenVerifier.run();
				});
				assert(verifierError instanceof BlocksVerificationError);
				assert(
					verifierError.message ===
						'Checksum does not match for range [0, 134217727]: "wrong" != "4c754335"',
				);
				await destination.close();
			},
		);

		// Test regular streams
		const sourceStream = await innerSource.createReadStream();
		await withTmpFile(
			{ keepOpen: false },
			async ({ path }: { path: string }) => {
				const destination = new sourceDestination.File({ path, write: true });
				await destination.open();
				const destinationStream = await destination.createWriteStream();
				await new Promise((resolve, reject) => {
					sourceStream.on('error', reject);
					destinationStream.on('error', reject);
					destinationStream.on('done', resolve);
					sourceStream.pipe(destinationStream);
				});
				// Test sparse verifier on file created from regular stream
				const verifier = destination.createVerifier(sourceSparseStream.blocks);
				await new Promise((resolve, reject) => {
					verifier.on('error', reject);
					verifier.on('finish', resolve);
					verifier.run();
				});
				await destination.close();
			},
		);
		await innerSource.close();
	});

	const checksumTypes: ChecksumType[] = ['sha1', 'sha256', 'crc32', 'xxhash3'];
	for (const alignment of [1, 1024 ** 2]) {
		for (const createStreamFromDisk of [false, true]) {
			for (const checksumType of checksumTypes) {
				it(`${checksumType} hasher, createStreamFromDisk=${createStreamFromDisk}, alignment=${alignment}`, async () => {
					const source = new sourceDestination.File({ path: DISK_PATH });
					const trimmedSource = new sourceDestination.ConfiguredSource({
						source,
						shouldTrimPartitions: true,
						createStreamFromDisk,
						checksumType,
						chunkSize: alignment,
					});
					await trimmedSource.open();
					const sourceSparseStream = await trimmedSource.createSparseReadStream(
						{ generateChecksums: true },
					);
					if (alignment === 1) {
						assert(sourceSparseStream.blocks.length === 15);
					} else {
						assert(sourceSparseStream.blocks.length === 1);
					}
					for (const block of sourceSparseStream.blocks) {
						assert(block.checksumType === checksumType);
						assert(block.checksum === undefined);
						assert(block.blocks.length === 1);
					}
					await withTmpFile(
						{ keepOpen: false },
						async ({ path }: { path: string }) => {
							const destination = new sourceDestination.File({
								path,
								write: true,
							});
							await destination.open();
							// Test sparse write stream
							const destinationStream = await destination.createSparseWriteStream();
							await new Promise((resolve, reject) => {
								sourceSparseStream.on('error', reject);
								destinationStream.on('error', reject);
								destinationStream.on('done', resolve);
								sourceSparseStream.pipe(destinationStream);
							});

							// Checksums should have been generated
							for (const block of sourceSparseStream.blocks) {
								assert(block.checksum !== undefined);
							}

							// Test sparse verifier for randomly readable destination
							// (This tests SparseReadStream)
							const verifier = destination.createVerifier(
								sourceSparseStream.blocks,
							);
							await new Promise((resolve, reject) => {
								verifier.on('error', reject);
								verifier.on('finish', resolve);
								verifier.run();
							});

							// Test sparse verifier for non randomly readable destination
							// (This tests SparseFilterStream)
							const canReadStub = stub(destination, 'canRead');
							canReadStub.resolves(false);
							const verifier2 = destination.createVerifier(
								sourceSparseStream.blocks,
							);
							await new Promise((resolve, reject) => {
								verifier2.on('error', reject);
								verifier2.on('finish', resolve);
								verifier2.run();
							});
							canReadStub.restore();

							// Test sparse verifier with wrong checksum
							const wrongBlocks = JSON.parse(
								JSON.stringify(sourceSparseStream.blocks),
							);
							wrongBlocks[0].checksum = 'wrong';
							const brokenVerifier = destination.createVerifier(wrongBlocks);
							const verifierError: Error = await new Promise(
								(resolve, reject) => {
									brokenVerifier.on('finish', () => {
										reject(
											new Error('There should have been a checksum mismatch'),
										);
									});
									brokenVerifier.on('error', (error: Error) => {
										resolve(error);
									});
									brokenVerifier.run();
								},
							);
							assert(verifierError instanceof BlocksVerificationError);
						},
					);
					await trimmedSource.close();
				});
			}
		}
	}

	it('blockmap in a zip file', async () => {
		const source = new sourceDestination.File({ path: ZIP_PATH });
		const innerSource = await source.getInnerSource();
		assert(innerSource instanceof sourceDestination.ZipSource);

		const sourceSparseStream = await innerSource.createSparseReadStream();
		assert(sourceSparseStream.blocks.length === 4);
		const checksums = [
			'ce55029ab3dd4875edbf69fcc5d1942bb2abaf2cf88b87ae7016609b3feb5028',
			'2d4be254f0330b3f01f89bc2dc2a1c69c8e0a196810bf3398cc5bb234ba6dd26',
			'5729edacf57e5785d73831d666b07009692a870159327a43366a788cd9fb5cd7',
			'ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7',
		];
		for (const [i, block] of sourceSparseStream.blocks.entries()) {
			assert(block.checksumType === 'sha256');
			assert(block.checksum === checksums[i]);
			assert(block.blocks.length === 1);
		}

		// Create a temporary destination file:
		await withTmpFile(
			{ keepOpen: false },
			async ({ path }: { path: string }) => {
				const destination = new sourceDestination.File({ path, write: true });
				await destination.open();
				// Test sparse write stream
				const destinationStream = await destination.createSparseWriteStream();
				await new Promise((resolve, reject) => {
					sourceSparseStream.on('error', reject);
					destinationStream.on('error', reject);
					destinationStream.on('done', resolve);
					sourceSparseStream.pipe(destinationStream);
				});

				// Test sparse verifier for randomly readable destination
				// (This tests SparseReadStream)
				const verifier = destination.createVerifier(sourceSparseStream.blocks);
				await new Promise((resolve, reject) => {
					verifier.on('error', reject);
					verifier.on('finish', resolve);
					verifier.run();
				});

				// Test sparse verifier for non randomly readable destination
				// (This tests SparseFilterStream)
				const canReadStub = stub(destination, 'canRead');
				canReadStub.resolves(false);
				const verifier2 = destination.createVerifier(sourceSparseStream.blocks);
				await new Promise((resolve, reject) => {
					verifier2.on('error', reject);
					verifier2.on('finish', resolve);
					verifier2.run();
				});
				canReadStub.restore();

				// Test sparse verifier with wrong checksum
				const wrongBlocks = JSON.parse(
					JSON.stringify(sourceSparseStream.blocks),
				);
				wrongBlocks[0].checksum = 'wrong';
				const brokenVerifier = destination.createVerifier(wrongBlocks);
				const verifierError: Error = await new Promise((resolve, reject) => {
					brokenVerifier.on('finish', () => {
						reject(new Error('There should have been a checksum mismatch'));
					});
					brokenVerifier.on('error', (error: Error) => {
						resolve(error);
					});
					brokenVerifier.run();
				});
				assert(verifierError instanceof BlocksVerificationError);
				assert(
					verifierError.message ===
						'Checksum does not match for range [0, 16383]: "wrong" != "ce55029ab3dd4875edbf69fcc5d1942bb2abaf2cf88b87ae7016609b3feb5028"',
				);
			},
		);
		await innerSource.close();
	});
});
