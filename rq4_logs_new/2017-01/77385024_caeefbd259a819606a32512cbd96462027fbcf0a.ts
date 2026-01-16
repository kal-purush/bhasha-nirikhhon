import { Directive, ElementRef, HostListener, Renderer } from '@angular/core';

@Directive({
  selector: '[app-change-image-with-default]'
})
export class ChangeImageWithDefaultDirective {
    private image_url: string;

    constructor(private elementRef: ElementRef, private renderer: Renderer) {
        this.image_url = 'https://s-media-cache-ak0.pinimg.com/736x/f0/26/05/f0260599e1251c67eefca31c02a19a81.jpg';
  }

    @HostListener('click') onItemClick() {
        // this.elementRef.nativeElement.classList.remove('prof-img');
        this.renderer.setElementAttribute(this.elementRef.nativeElement, 'src', null);
        this.renderer.setElementStyle(this.elementRef.nativeElement, 'background-image', 'url(' + this.image_url + ')');
        this.renderer.setElementStyle(this.elementRef.nativeElement, 'background-size', '140px 140px');
    }
}