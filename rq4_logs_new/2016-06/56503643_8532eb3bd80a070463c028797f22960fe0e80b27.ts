/**
 * Animation
 */
class Animation {
    private htmlElement:    HTMLElement;
    
    private frameWidth:     number = 0;
    private frameHeight:    number = 0;
    private animationSpeed: number = 0;
    private currentFrame:   number = 0;
    private maxFrames:      number = 0;
    
    private timer : number = 0;
    
    constructor(htmlElement: HTMLElement, frameWidth: number, frameHeight: number, animationSpeed: number, maxFrames: number) {
        this.htmlElement    = htmlElement;
        this.frameWidth     = frameWidth;
        this.frameHeight    = frameHeight;
        this.animationSpeed = animationSpeed;
        this.maxFrames      = maxFrames;
    }
    
    public update() : void {
        this.timer++;
        
        if(this.timer % this.animationSpeed == 0) 
            this.currentFrame++;
        if(this.currentFrame > this.maxFrames - 1) 
            this.currentFrame = 0;
    }
    
    public draw() : void {
        this.htmlElement.style.backgroundPosition = (this.currentFrame * -this.frameWidth) + "px 0px";
    }
}