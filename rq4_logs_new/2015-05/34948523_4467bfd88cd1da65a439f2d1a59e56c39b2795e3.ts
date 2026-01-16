class CeilingFan {
    private location = '';

    private level: number;
    static HIGH = 2;
    static MEDIUM = 1;
    static LOW = 0;

    constructor(location: string) {
        this.location = location;
    }

    high(): void {
        this.level = CeilingFan.HIGH;
        console.log(this.location + ' のシーリングファンの強さは「強」です');
    }

    medium(): void {
        this.level = CeilingFan.MEDIUM;
        console.log(this.location + ' のシーリングファンの強さは「中」です');
    }

    low(): void {
        this.level = CeilingFan.LOW;
        console.log(this.location + ' のシーリングファンの強さは「弱」です');
    }

    off(): void {
        this.level = 0;
        console.log(this.location + ' のシーリングファンは止まっています');
    }

    getSpeed(): number {
        return this.level;
    }
}