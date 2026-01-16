declare class DFT {
    /**
    * DFT is a class for calculating the Discrete Fourier Transform of a signal.
    *
    * @param {Number} bufferSize The size of the sample buffer to be computed
    * @param {Number} sampleRate The sampleRate of the buffer (eg. 44100)
    *
    * @constructor
    */
    constructor(bufferSize: number, sampleRate: number);
    /**
    * Performs a forward transform on the sample buffer.
    * Converts a time domain signal to frequency domain spectra.
    *
    * @param {Array} buffer The sample buffer
    *
    * @returns The frequency spectrum array
    */
    forward(buffer: Array<number>);

}

declare class FFT {
    /**
    * FFT is a class for calculating the Discrete Fourier Transform of a signal
    * with the Fast Fourier Transform algorithm.
    *
    * @param {Number} bufferSize The size of the sample buffer to be computed. Must be power of 2
    * @param {Number} sampleRate The sampleRate of the buffer (eg. 44100)
    *
    * @constructor
    */
    constructor(bufferSize: number, sampleRate: number);
    /**
    * Performs a forward transform on the sample buffer.
    * Converts a time domain signal to frequency domain spectra.
    *
    * @param {Array} buffer The sample buffer. Buffer Length must be power of 2
    *
    * @returns The frequency spectrum array
    */
    forward(buffer: Array<number>): Array<number>;

    inverse(real: number, imag: number);
}

declare class RFFT {
    /**
    * RFFT is a class for calculating the Discrete Fourier Transform of a signal
    * with the Fast Fourier Transform algorithm.
    *
    * This method currently only contains a forward transform but is highly optimized.
    *
    * @param {Number} bufferSize The size of the sample buffer to be computed. Must be power of 2
    * @param {Number} sampleRate The sampleRate of the buffer (eg. 44100)
    *
    * @constructor
    */
    constructor(bufferSize: number, sampleRate: number);
    /**
    * Performs a forward transform on the sample buffer.
    * Converts a time domain signal to frequency domain spectra.
    *
    * @param {Array} buffer The sample buffer. Buffer Length must be power of 2
    *
    * @returns The frequency spectrum array
    */
    forward(buffer: Array<number>): Array<number>;
}

declare class Sampler {
    constructor(file, bufferSize, sampleRate, playStart, playEnd, loopStart, loopEnd, loopMode);

    applyEnvelope();
    generate();
    setFreq(frequency: number);
    reset();
}

declare class Oscillator {
    /**
    * Oscillator class for generating and modifying signals
    *
    * @param {Number} type       A waveform constant (eg. DSP.SINE)
    * @param {Number} frequency  Initial frequency of the signal
    * @param {Number} amplitude  Initial amplitude of the signal
    * @param {Number} bufferSize Size of the sample buffer to generate
    * @param {Number} sampleRate The sample rate of the signal
    *
    * @contructor
    */
    constructor(type: number, frequency: number, amplitude: number, bufferSize: number, sampleRate: number);
    
    /**
    * Set the amplitude of the signal
    *
    * @param {Number} amplitude The amplitude of the signal (between 0 and 1)
    */
    setAmp(amplitude: number);
    /**
    * Set the frequency of the signal
    *
    * @param {Number} frequency The frequency of the signal
    */ 
    setFreq(frequency: number);
    add(oscillator: Oscillator);
    addSignal(signal: Array<number>);
    addEnvelope(envelope: any);
    applyEnvelope();
    valueAt(offset: number);
    generate();
}