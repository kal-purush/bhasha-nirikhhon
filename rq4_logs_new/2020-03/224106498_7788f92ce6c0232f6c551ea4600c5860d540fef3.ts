// It's a polyfill, so we must be checking for the presence of an unbound method
// that would normally be there
/* eslint-disable @typescript-eslint/unbound-method */
/* eslint-disable @typescript-eslint/no-unnecessary-condition */

interface AudioBuffer {
    copyToChannel(source: Float32Array, channelNumber: number, startInChannel?: number): void;
}

if (!AudioBuffer.prototype.copyToChannel) {
    Object.defineProperty(AudioBuffer.prototype, "copyToChannel", {
        enumerable: false,
        configurable: false,
        writable: false,
        value: function(this: AudioBuffer, source: Float32Array, channelNumber: number, startInChannel: number = 0): void {
            const arr = this.getChannelData(channelNumber);
            const len = Math.min(arr.length, source.length);
            for (let i = startInChannel, j = 0; j < len; i++, j++) {
                arr[i] = source[j];
            }
        }
    });
}