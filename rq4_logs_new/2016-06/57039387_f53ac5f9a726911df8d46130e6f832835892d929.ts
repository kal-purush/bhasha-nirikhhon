import Game from "./Game";

export default class AudioMgr
{
    private _game: Game;
    private _audio: string[];
    private _loaded: number;

    constructor(g: Game)
    {
        this._game = g;
        this._loaded = 0;

        this._audio = [
            // insert paths to audio files
        ];

        this.loadImages();
    }

    private loadImages(): void
    {
        let fn = this.audioLoaded.bind(this);

        for (var i = 0; i < this._audio.length; i += 1) {
            var audio = new Audio();
            audio.oncanplaythrough = fn;
            audio.src = this._audio[i];
        }
    }

    private audioLoaded(): void
    {
        this._loaded += 1;

        if (this._loaded == this._audio.length) {
            console.log('starting game');
            this._game.startGame();
        }
    }
}