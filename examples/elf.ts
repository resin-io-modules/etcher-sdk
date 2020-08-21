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
import { Argv } from 'yargs';

import { File } from '../lib/source-destination/file';
import { ConfiguredElf } from '../lib/source-destination/configured-source/configured-source';

import { pipeSourceToDestinationsWithProgressBar, wrapper } from './utils';

async function main({
	executable,
	sectionName,
	configFile,
	outputFile,
}: {
	executable: string;
	sectionName: string;
	configFile: string;
	outputFile: string;
}) {
	const elf = new ConfiguredElf({
		source: new File({ path: executable }),
		sectionName,
		configJson: await fs.readFile(configFile, { encoding: 'utf8' }),
	});
	const output = new File({ path: outputFile, write: true });
	await pipeSourceToDestinationsWithProgressBar({
		source: elf,
		destinations: [output],
		trim: false,
		decompressFirst: false,
	});
}

// tslint:disable-next-line: no-var-requires
const argv = require('yargs').command(
	'$0 <executable> <sectionName> <configFile> <outputFile>',
	'Injects configFile into the sectionName section of the elf executable.',
	(yargs: Argv) => {
		yargs.positional('executable', {
			describe: 'Elf file where the config will be injected',
		});
		yargs.positional('sectionName', {
			describe:
				'Name of the section of the elf file where the config will be injected',
		});
		yargs.positional('configFile', { describe: 'Config file to inject' });
		yargs.positional('outputFile', { describe: 'Output elf file' });
	},
).argv;

wrapper(main, argv);
