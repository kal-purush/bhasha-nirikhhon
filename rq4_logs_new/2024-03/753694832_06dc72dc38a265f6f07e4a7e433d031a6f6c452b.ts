import DataFilterer from '../DataFilterer';
import SetElement from '../setutils/SetElement';
import { stringReplacer } from './csvreader';

class Uint8Buffer {
    public buffer: ArrayBuffer = new ArrayBuffer(8000);
    public accessibleBuffer: Uint8Array = new Uint8Array(this.buffer);
    public usedLength: number = 0;
}

// Returns a resized buffer and works on new browsers
function resizedBufferNew(buf: Uint8Buffer, length) {
    // @ts-expect-error: use this only when supported, check first before using
    buf.buffer = buf.buffer.transferToFixedLength(length);
    buf.accessibleBuffer = new Uint8Array(buf.buffer);
}

// Returns a resized buffer but is slower
function resizedBufferOld(buf: Uint8Buffer, length) {
    const oldAccessibleBuffer = buf.accessibleBuffer;
    buf.buffer = new ArrayBuffer(length);
    buf.accessibleBuffer = new Uint8Array(buf.buffer);
    buf.accessibleBuffer.set(oldAccessibleBuffer);
}

// @ts-expect-error: don't use the function here but only check if it exists, only then use it
const bufferResizer = new ArrayBuffer(1).transferToFixedLength
    ? resizedBufferNew
    : resizedBufferOld;

function allocateMoreForAddition(base: Uint8Buffer, extensionLength: number) {
    // Resize if needed
    if (base.usedLength + extensionLength > base.buffer.byteLength) {
        bufferResizer(
            base,
            Math.max(base.buffer.byteLength * 2, base.usedLength + extensionLength)
        );
    }
}

const utf8encoder = new TextEncoder();

export function exportCSV(filterer: DataFilterer): Uint8Array {
    if (!utf8encoder.encodeInto) {
        window.alert('Browser too old, exporting CSVs not supported');
        return new Uint8Array();
    }
    const buffer = new Uint8Buffer();

    const columns = filterer.getColumns();
    const [data, l] = filterer.getData();
    const columnCount = columns.length;

    for (let i = 0; i < columnCount; i++) {
        const cell = '"' + stringReplacer(columns[i], '"', '""') + '",';

        // Follow Mozilla docs for TextEncoder.
        allocateMoreForAddition(buffer, cell.length * 4);
        // Follow Mozilla docs for encoding: use subarray to specify starting index
        const { written } = utf8encoder.encodeInto(
            cell,
            buffer.accessibleBuffer.subarray(buffer.usedLength)
        );

        buffer.usedLength += written;
    }
    buffer.accessibleBuffer[buffer.usedLength - 1] = 10; //'\n' as unknown as number; // replace the final ',' with \n

    // See the above loop; this one is almost same but uncommented
    for (let i = 0; i < l; i++) {
        for (const cell of data[i]) {
            if (cell instanceof SetElement) {
                const str = cell.value;
                const str2 =
                    '"' + (str.includes('"') ? stringReplacer(str, '"', '""') : str) + '",';
                allocateMoreForAddition(buffer, str2.length * 4);
                const { written } = utf8encoder.encodeInto(
                    str2,
                    buffer.accessibleBuffer.subarray(buffer.usedLength)
                );
                buffer.usedLength += written;
            } else {
                const str = '"' + cell;
                const str2 =
                    (str.includes('"', 1) ? '"' + stringReplacer('' + cell, '"', '""') : str) +
                    '",';
                allocateMoreForAddition(buffer, str2.length * 4);
                const { written } = utf8encoder.encodeInto(
                    str2,
                    buffer.accessibleBuffer.subarray(buffer.usedLength)
                );
                buffer.usedLength += written;
            }
        }
        buffer.accessibleBuffer[buffer.usedLength - 1] = 10; //'\n' as unknown as number;
    }
    return new Uint8Array(buffer.buffer, 0, buffer.usedLength);
}