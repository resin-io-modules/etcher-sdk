declare module 'readable-stream' {
	import { EventEmitter } from 'events';
	import {
		PassThrough as NodeJSPassThrough,
		Readable as NodeJSReadable,
		Transform as NodeJSTransform,
		Writable as NodeJSWritable,
	} from 'stream';

	export class PassThrough extends NodeJSPassThrough {}

	export class Readable extends NodeJSReadable {}

	export class Transform extends NodeJSTransform {}

	export class Writable extends NodeJSWritable {}
}
