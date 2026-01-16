import { Component, OnInit, ViewChild, ElementRef, AfterViewInit } from '@angular/core';
import { AdminoScreenElement } from '../admino-screen-element';

@Component({
  selector: 'admino-image',
  templateUrl: './image.component.html',
  styleUrls: ['./image.component.scss']
})
export class ImageComponent extends AdminoScreenElement implements OnInit, AfterViewInit {
  @ViewChild('imageRef', { static: true, read: ElementRef }) imageRef: ElementRef;
  loadedImage;
  imgWidth = 0;
  imgHeight = 0;
  ngOnInit() {
  }

  ngAfterViewInit() {

    this.onChange(null);
  }
  onChange(changes: any) {
    this.loadImage();
    this.loadedImage.src = this.element.src;
    // this.screen.update(this.element);
  }

  loadImage() {
    if (this.loadedImage) {
      this.loadedImage.onload = null;
      this.loadedImage = null;
    }
    this.loadedImage = new Image();
    this.loadedImage.onload = (img) => {
      // _img.src = this.src;
      const maxW = this.imageRef.nativeElement.parentNode.parentNode.clientWidth;
      let w = this.loadedImage.width;
      let h = this.loadedImage.height;
      if (w > maxW) {
        w = maxW;
        h = this.loadedImage.height * (maxW / this.loadedImage.width);
      }
      this.imgWidth = this.element.width !== undefined ? this.element.width : w + 'px';
      this.imgHeight = this.element.height !== undefined ? this.element.height : h + 'px';
    };

  }


}