export default class Sound {
    private context: AudioContext;
    private buffer: AudioBuffer;
    private source: AudioBufferSourceNode
    private playing: boolean;

    constructor(context: AudioContext, buffer: AudioBuffer, source: AudioBufferSourceNode) {
        this.context = context;
        this.buffer = buffer;
        this.source = source;
    }

    public isPlaying() {
        return this.playing;
    }

    public setup() {
        if (this.source.buffer) {
            this.source = this.context.createBufferSource();
        }
        this.source.buffer = this.buffer;
        this.source.connect(this.context.destination);
        this.source.loop = true;
    }

    public play() {
        this.playing = true;
        this.setup();
        this.source.start(this.context.currentTime);
        this.source.onended = () => {
            this.playing = false;
        }
    }

    public stop() {
        this.source.stop(this.context.currentTime);
        this.playing = false;
    }
}