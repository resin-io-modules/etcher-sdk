declare module 'xxhash-addon' {
	import { Hash } from 'crypto';
	import { Transform } from 'stream';

	class XXHashAddon {
		constructor(seed: number);
		public update(buffer: Buffer): void;
		public digest(encoding: string): string;
	}

	namespace XXHashAddon {
		export class Stream extends Transform {
			constructor(seed: number, bits?: number, enc?: string);
		}

		export class XXHash64 {
			constructor(seed: number);
			public update(buffer: Buffer): void;
			public digest(encoding: string): string;
		}

		export class XXHash3 {
			constructor(seed: number);
			public update(buffer: Buffer): void;
			public digest(encoding?: string): string;
		}
	}

	export = XXHashAddon;
}
