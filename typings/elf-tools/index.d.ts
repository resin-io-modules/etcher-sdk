declare module 'elf-tools' {
	export interface ELF {
		header: {
			endian: 'lsb' | 'msb';
		};
		sections: Array<{
			header: {
				offset: number;
				size: number;
				name: string;
			};
		}>;
	}

	export function parse(buffer: Buffer): ELF;
}
