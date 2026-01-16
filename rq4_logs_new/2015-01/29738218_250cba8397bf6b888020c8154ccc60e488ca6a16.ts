export class Oscillator {
    private static waveTable: { [key: string]: Float32Array };

    public static Sine = function (step) {
        return Math.sin(2 * Math.PI * step);
    };

    public static Cosine = function (step) {
        return Math.cos(2 * Math.PI * step);
    };

    public static Square = function (step) {
        return step < 0.5 ? 1 : -1;
    };

    public static Saw = function (step) {
        return 2 * (step - Math.round(step));
    };

    public static Triangle = function (step) {
        return 1 - 4 * Math.abs(Math.round(step) - step);
    };

    public static Pulse = function (step) {
        // stub
    };

    private _frequency: number;
    private _amplitude: number;
    private _bufferSize: number;
    private _sampleRate: number;
    private _frameCount: number;
    private _waveTableLength: number;
    private _cyclesPerSample: number;
    private _signal: Float32Array;
    private _envelope: number;
    private _func: any;


    private _waveTable: Float32Array;

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
    constructor(type: OscillatorType, frequency: number, amplitude: number, bufferSize: number, sampleRate: number) {
        this._frequency = frequency;
        this._amplitude = amplitude;
        this._bufferSize = bufferSize;
        this._sampleRate = sampleRate;
        this._frameCount = 0;

        this._waveTableLength = 2048;

        this._cyclesPerSample = frequency / sampleRate;

        this._signal = new Float32Array(bufferSize);
        this._envelope = null;

        switch (type) {
            case OscillatorType.Cosine:
                this._func = Oscillator.Cosine;
                break;
            case OscillatorType.Triangle:
                this._func = Oscillator.Triangle;
                break;

            case OscillatorType.Saw:
                this._func = Oscillator.Saw;
                break;

            case OscillatorType.Square:
                this._func = Oscillator.Square;
                break;

            default:
            case OscillatorType.Sine:
                this._func = Oscillator.Sine;
                break;
        }

        if (typeof Oscillator.waveTable === 'undefined') {
            Oscillator.waveTable = {};
        }

        if (typeof Oscillator.waveTable[this._func] === 'undefined') {
            this._generateWaveTable();
        }

        this._waveTable = Oscillator.waveTable[this._func];
    }

    private _generateWaveTable() {
        Oscillator.waveTable[this._func] = new Float32Array(2048);
        var waveTableTime = this._waveTableLength / this._sampleRate;
        var waveTableHz = 1 / waveTableTime;

        for (var i = 0; i < this._waveTableLength; i++) {
            Oscillator.waveTable[this._func][i] = this._func(i * waveTableHz / this._sampleRate);
        }
    }
}