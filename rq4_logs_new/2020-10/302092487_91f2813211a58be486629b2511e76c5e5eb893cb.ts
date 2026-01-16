import { Injectable } from '@angular/core';

import { Product } from '../models/product';
import { ProductType } from '../models/product-type';

@Injectable({
    providedIn: 'root'
})

export class ProductService {
    private productTypes: ProductType[] = [
        {
            id: '1',
            name: 'vegetable',
            images: 'https://img1.mashed.com/img/uploads/2017/07/vegetables.jpg'
        },
        {
            id: '2',
            name: 'fruit',
            images: 'https://upload.wikimedia.org/wikipedia/commons/2/2f/Culinary_fruits_front_view.jpg'
        },
    ]
    private products: Product[] = [
        {
            id: 'v1',
            type: 'vegetable',
            name: 'carrot',
            price: 100,
            images: 'https://i5.walmartimages.ca/images/Enlarge/686/686/6000198686686.jpg',
            quantity: 100
        },
        {
            id: 'v2',
            type: 'vegetable',
            name: 'potato',
            price: 1230,
            images: 'https://api.time.com/wp-content/uploads/2020/04/Boss-Turns-Into-Potato.jpg',
            quantity: 10
        },
        {
            id: 'v3',
            type: 'vegetable',
            name: 'tomato',
            price: 13120,
            images: 'https://upload.wikimedia.org/wikipedia/commons/a/a2/Tomato.jpg',
            quantity: 13120
        },
        {
            id: 'v4',
            type: 'vegetable',
            name: 'corn',
            price: 100,
            images: 'http://bayaderae.com/wp-content/uploads/2013/02/Corn.jpg',
            quantity: 100
        },
        {
            id: 'v5',
            type: 'vegetable',
            name: 'pumpkin',
            price: 100,
            images: 'https://gamepedia.cursecdn.com/dayz_gamepedia/6/64/Pumpkin.png',
            quantity: 100
        },
        {
            id: 'f1',
            type: 'fruit',
            name: 'apple',
            price: 100,
            images: 'https://mccutcheonsblog.files.wordpress.com/2011/09/red_delicious_apple.jpg',
            quantity: 100
        },
        {
            id: 'f2',
            type: 'fruit',
            name: 'banana',
            price: 1230,
            images: 'https://www.pharmamirror.com/wp-content/uploads/2013/06/Banna-as-Hepatitis-Oral-Vaccine.jpg',
            quantity: 10
        },
        {
            id: 'f3',
            type: 'fruit',
            name: 'watermelon',
            price: 13120,
            images: 'https://all-americaselections.org/wp-content/uploads/2019/06/Watermelon-Mambo.jpg',
            quantity: 13120
        },
        {
            id: 'f4',
            type: 'fruit',
            name: 'strawberry',
            price: 100,
            images: 'https://gamepedia.cursecdn.com/atlas_gamepedia_en/6/6d/Strawberry.png',
            quantity: 100
        },
        {
            id: 'f5',
            type: 'fruit',
            name: 'grapes',
            price: 100,
            images: 'https://gamepedia.cursecdn.com/atlas_gamepedia_en/6/6d/Strawberry.png',
            quantity: 100
        }
    ];

    constructor() {
    }

    getProducts() {
        /// ...syntax means return the copy of the array but not the array itself ???
        return [...this.products];
    }

    getProduct(id: string) {
        /// ...syntax means return the copy of the array but not the array itself ???
        return {...this.products.find(product => {
            return id === product.id;
        })};
    }

    getProductTypes(){
        return [...this.productTypes];
    }

    getProductType(id: string) {
        /// ...syntax means return the copy of the array but not the array itself ???
        return {...this.productTypes.find(productTypes => {
            return id === productTypes.id;
        })};
    }
}