import { Component } from 'angular2/core';

@Component({
    selector: 'kg-carousel',
    templateUrl: 'app/carousel/carousel.component.html'
})

export class CarouselComponent {
    images: CarouselImage[] = [
        { 
            title: "Welcome to Konki Gaming",
            description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse velit urna, pulvinar ac feugiat non, blandit quis augue. Praesent in eros sed tortor gravida iaculis vitae dictum tellus.",
            imgSrc: "app/assets/images/image1.jpg",
            link: "home"
        },
        { 
            title: "Bad boys for life",
            description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse velit urna, pulvinar ac feugiat non, blandit quis augue. Praesent in eros sed tortor gravida iaculis vitae dictum tellus.",
            imgSrc: "app/assets/images/image2.jpg",
            link: "videos"
        },
        { 
            title: "We killed kenny",
            description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse velit urna, pulvinar ac feugiat non, blandit quis augue. Praesent in eros sed tortor gravida iaculis vitae dictum tellus.",
            imgSrc: "app/assets/images/image3.jpg",
            link: "contact"
        },
        { 
            title: "Be strong little man",
            description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse velit urna, pulvinar ac feugiat non, blandit quis augue. Praesent in eros sed tortor gravida iaculis vitae dictum tellus.",
            imgSrc: "app/assets/images/image4.jpg",
            link: "teamsnplayers"
        }  
    ];
    activeImage: number = 0;
    
    setActiveImage(index): void {
        this.activeImage = index;
    };
}

interface CarouselImage {
    title: string;
    description: string;
    imgSrc: string;
    link: string;
}