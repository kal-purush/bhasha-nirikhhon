import {Component} from '@angular/core';
import {Http} from '@angular/http';

@Component({
    selector: "carousel",
    templateUrl: './carousel.component.html',
    styleUrls: [ './carousel.component.css' ]
})

export class CarouselComponent {
    private start = false;
    urls = ['https://static.wixstatic.com/media/279a9c5b1081428e8ad4d6dc3085d93e.jpg/v1/fill/w_810,h_379,al_c,q_80,usm_0.66_1.00_0.01/279a9c5b1081428e8ad4d6dc3085d93e.jpg',
                'https://static.wixstatic.com/media/b08b5f0f1977452da106294351c7075b.jpg/v1/fill/w_811,h_379,al_c,q_80,usm_0.66_1.00_0.01/b08b5f0f1977452da106294351c7075b.jpg',
                'https://static.wixstatic.com/media/9d7c49d59405474e93ca6fee5167840a.jpg/v1/fill/w_810,h_379,al_c,q_80,usm_0.66_1.00_0.01/9d7c49d59405474e93ca6fee5167840a.jpg'];
    
    constructor() {
        this.startCarousel(this.urls);         
    }

    startCarousel(urls: string[]) {
        this.urls = urls;
        $('.carousel').carousel();
    }

    isActive(url: string) {
        return url === this.urls[0];
    }
}

    