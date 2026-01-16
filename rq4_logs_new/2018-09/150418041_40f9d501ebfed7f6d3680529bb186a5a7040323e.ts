export class CountdownObject{

  private now: number;
  private completed: boolean;
  private secondsPassed: number;

  private startTime: number;
  private lastTick: number;

  constructor(public name: string, public seconds: number, public paused: boolean){
    this.now = new Date().getTime() / 1000;

    this.completed = false;
    this.secondsPassed = 0;
  }

  public start(): void{
    this.startTime = new Date().getTime() / 1000;
  }

  public tick(timeInSeconds: number): void{
    let timePassed
  }

  public getMinutes(): string{

    if(this.now > this.end){
      return "00";
    }else{
      return new String(Math.floor((this.end - this.now) / 60));
    }
  }

  public getSeconds(): string{

    if(this.now > this.end){
      return "00";
    }else{
      let minutes = Math.floor((this.end - this.now) / 60);
      let seconds = (minutes * 60) - (this.end - this.now);

      if(seconds < 0){
        return "00";
      }else if(seconds < 10){
        return "0" + new String(seconds);
      }else {
        return new String(seconds);
      }
    }
  }
}