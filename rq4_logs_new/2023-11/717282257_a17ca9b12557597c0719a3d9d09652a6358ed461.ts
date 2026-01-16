const decoder = new TextDecoder();

const pngSignature = 'PNG\r\n\x1a\n';
const pngImageHeaderChunkName = 'IHDR';
const pngFriedChunkName = 'CgBI';

export function validate(buffer: Buffer) {
    if (pngSignature === toUTF8String(buffer, 1, 8)) {
        let chunkName = toUTF8String(buffer, 12, 16);
        if (chunkName === pngFriedChunkName) chunkName = toUTF8String(buffer, 28, 32);
        if (chunkName !== pngImageHeaderChunkName) throw new TypeError('Invalid PNG');
        return true;
    }
    return false;
}

export function calculate(buffer: Buffer) {
    if (toUTF8String(buffer, 12, 16) === pngFriedChunkName) {
        return {
            height: readUInt32BE(buffer, 36),
            width: readUInt32BE(buffer, 32)
        };
    }
    return {
        height: readUInt32BE(buffer, 20),
        width: readUInt32BE(buffer, 16)
    }
}

export function sizeOf(buffer: Buffer): { width: number, height: number } {
    if (!(buffer instanceof Uint8Array)) throw new TypeError('buffer must be of type Uint8Array');
    if (!validate(buffer)) throw new TypeError('Invalid PNG');
    return calculate(buffer);
}

export function readUInt32BE(input: Uint8Array, offset = 0) {
    return input[offset] * 2 ** 24 + input[offset + 1] * 2 ** 16 + input[offset + 2] * 2 ** 8 + input[offset + 3];
}

export function toUTF8String(input: Uint8Array, start = 0, end = input.length) {
    return decoder.decode(input.slice(start, end));
}