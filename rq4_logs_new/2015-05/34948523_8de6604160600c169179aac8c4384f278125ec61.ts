class CeilingFan {
    static HIGH = 3;
    static MEDIUM = 2;
    static LOW = 1;
    static OFF = 0;

    private location: string;
    private _speed: number;

    constructor(location: string) {
        this.location = location;
        this._speed = CeilingFan.OFF;
    }

    high(): void {
        this._speed = CeilingFan.HIGH;
        console.log(this.location + ' のシーリングファンの強さは「強」です');
    }

    medium(): void {
        this._speed = CeilingFan.MEDIUM;
        console.log(this.location + ' のシーリングファンの強さは「中」です');
    }

    low(): void {
        this._speed = CeilingFan.LOW;
        console.log(this.location + ' のシーリングファンの強さは「弱」です');
    }

    off(): void {
        this._speed = CeilingFan.OFF;
        console.log(this.location + ' のシーリングファンはついていません');
    }

    get speed(): number {
        return this._speed;
    }
}