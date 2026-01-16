/// <reference path="../shared/include.ts" />
/// <reference path="MusicalNote.ts" />

class Bubble
{
    // readonly
    public radius: number;
    public baseFrequency: number;
    public actualFrequency: number;
    
    // dynamic
    public location: Vector2D;
    public life: number;
    
    public constructor(
        public note: MusicalNote,
        worldSize: Vector2D)
    {
        this.baseFrequency = 220 * Math.pow(Math.pow(2, 1/12), note.note); // 220..440
        this.actualFrequency = Math.pow(2, note.octave) * this.baseFrequency;
        this.radius = 20000 / this.actualFrequency;
        
        this.life = note.duration;
        
        this.location = {
            x: Math.random() * (worldSize.x - 2 * this.radius) - worldSize.x / 2 + this.radius,
            y: worldSize.y / 2 + this.radius
        };
    }
    
    public move(step: number): void
    {
        this.location.y -= step;
    }
    
    public hit(elapsedMS: number): void
    {
        this.life -= elapsedMS;
    }
}

export = Bubble;