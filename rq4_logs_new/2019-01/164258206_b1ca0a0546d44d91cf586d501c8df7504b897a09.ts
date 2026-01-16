import { Component, OnInit } from '@angular/core';
import { CarouselService } from 'src/app/providers/shared/carousel.service';
import { CarouselImage } from '../../../interfaces/carousel-image.interface';

@Component({
  selector: 'app-carousel',
  templateUrl: './carousel.component.html',
  styles: []
})
export class CarouselComponent implements OnInit {

  images: CarouselImage[];

  constructor(private carouselService: CarouselService) {
    this.carouselService.getImages().subscribe((images: CarouselImage[]) => this.images = images);
  }

  ngOnInit() {
  }

}