import {
  Directive,
  Renderer2,
  OnInit,
  ElementRef,
  HostListener,
  HostBinding,
  Input
} from "@angular/core";

@Directive({
  selector: "[appBetterHighlight]"
})
export class BetterHighlightDirective implements OnInit {
  @Input() defaultColor: string = "transparent";
  @Input() highlightColor: string = "blue";

  @HostBinding("style.backgroundColor") backgroundColor: string;

  constructor(private elRef: ElementRef, private renderer: Renderer2) {}

  ngOnInit() {
    // this.renderer.setStyle(
    //   this.elRef.nativeElement, "background-color", "blue"
    // );
    // If you wanna set the style as important you can set a forth argument about this!

    this.backgroundColor = this.defaultColor;
  }

  @HostListener("mouseenter") mouseover(eventData: Event) {
    // this.renderer.setStyle(
    //   this.elRef.nativeElement, "background-color", "blue"
    // );
    this.backgroundColor = this.highlightColor;
  }

  @HostListener("mouseleave") mouseleave(eventData: Event) {
    // this.renderer.setStyle(
    //   this.elRef.nativeElement, "background-color", "transparent"
    // );
    this.backgroundColor = this.defaultColor;
  }
}
// It is a better practice to use the renderer than to set the style straight away.
// If you use the direct way in some cases (if you don't run the app in a browser), it's posible that it doesn't get the style right!