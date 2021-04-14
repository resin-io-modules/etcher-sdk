/*
Copyright 2020 balena.io

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import { promisify } from 'util';
import * as xxhash from 'xxhash-addon';

import { once } from './utils';

export type XXHash = typeof xxhash;

export const getRaspberrypiUsbboot = once(() => {
	try {
		return require('node-raspberrypi-usbboot') as typeof import('node-raspberrypi-usbboot');
	} catch (e) {
		console.warn('Failed to import node-raspberrypi-usbboot:', e);
	}
});

export const getXXHash = once(
	() => require('xxhash-addon') as typeof import('xxhash-addon'),
);

export const getUnmountDisk = once(() =>
	promisify((require('mountutils') as typeof import('mountutils')).unmountDisk),
);

export const getCrc = once(
	() => require('cyclic-32') as typeof import('cyclic-32'),
);
