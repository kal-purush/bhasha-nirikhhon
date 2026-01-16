import { Directive, OnInit, ElementRef, Renderer2 } from '@angular/core';

@Directive({
  selector: '[appBetterBlueHighLight]'
})
export class BetterBlueHighLightDirective implements OnInit {

  constructor(private elemRef:ElementRef,private renderer:Renderer2) { }
ngOnInit() {
  this.renderer.setStyle(this.elemRef.nativeElement,'color','yellow')
}

}