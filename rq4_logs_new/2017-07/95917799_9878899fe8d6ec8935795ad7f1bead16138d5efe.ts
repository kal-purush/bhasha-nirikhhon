/**
 * Created by Ajit on 04-07-2017.
 */

import {ElementRef} from "@angular/core";

export class Slider {
  private containerSize: number = 0;
  private path:string = "assets/images/";
  private imgArr:Array<string> = ["banner.png","banner2.jpg","banner3.jpg"];
  private arrow:number = 0;

  constructor(private container:ElementRef) {
    this.containerSize = this.container.nativeElement.clientWidth;
    this.loopSlides();
  }

  public updateSize():void {
    this.containerSize = this.container.nativeElement.clientWidth;
  }

  public leftMove():void {
    if(this.arrow == 0 || this.arrow < 0) this.arrow = 2;
    else this.arrow -= 1

    console.log(this.arrow);
    this.container.nativeElement.style.backgroundImage = "url("+this.path+this.imgArr[this.arrow]+")";
  }

  public rightMove():void {
    if(this.arrow == 2 || this.arrow > 2) this.arrow = 0;
    else this.arrow += 1

    this.container.nativeElement.style.backgroundImage = "url("+this.path+this.imgArr[this.arrow]+")";
  }

  private loopSlides():void {
    let loader = 0;
    setInterval(()=>{
      loader += 1;
      if(loader > 2) loader = 0
      this.container.nativeElement.style.backgroundImage = "url("+this.path+this.imgArr[loader]+")";
    },2000);
  }
}