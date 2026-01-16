import suit1 from '@/assets/suit1.webp'
import suit2 from '@/assets/suit2.jpg'
import suit3 from '@/assets/suit3.webp'

import coat1 from '@/assets/coat1.webp'
import coat2 from '@/assets/coat2.jpg'
import coat3 from '@/assets/coat3.jpg'

import shirt1 from '@/assets/shirt1.jpg'
import shirt2 from '@/assets/shirt2.webp'
import shirt3 from '@/assets/shirt3.jpg'

import dress1  from '@/assets/dress1.jpg'
import dress2  from '@/assets/dress2.jpg'
import dress3  from '@/assets/dress3.jpg'

import pant1 from '@/assets/dress1.jpg'
import pant2 from '@/assets/dress2.jpg'
import pant3 from '@/assets/dress3.jpg'



export type ItemType = {
    id:string,
    name:string,
    description: string,
    price:number,
    category: 'dress' | 'coat' | 'shirt' | 'pant' | 'suit'
    image:any
}


export const Items: ItemType[] = [
    {
        id: 'd692ec85-6e0a-4e4b-bd8a-1b8b43f32e5e',
        category: 'dress',
        description: 'Elegant dress with intricate design, epitomizing timeless fashion.',
        image: dress1,
        name: 'dress1',
        price: 99.9
    },
    {
        id: 'a7b85a1d-5e54-47cb-aa04-9cf4c3a7a107',
        category: 'dress',
        description: 'Versatile dress, perfect for expressing personal style.',
        image: dress2,
        name: 'dress2',
        price: 99.9
    },
    {
        id: 'e59bb78c-d4a6-44c2-bd84-3b237e7f4fe7',
        category: 'dress',
        description: 'Celebratory dress, ideal for special occasions and events.',
        image: dress3,
        name: 'dress3',
        price: 99.9
    },
    {
        id: '1dc62c3b-4510-45e4-9b02-92d6d94c32a2',
        category: 'coat',
        description: 'Classic coat with modern details, perfect for chilly days.',
        image: coat1,
        name: 'coat1',
        price: 129.9
    },
    {
        id: '8d7f5405-7a27-4b5d-af7d-3e648f9b96db',
        category: 'coat',
        description: 'Stylish coat to keep you warm and fashionable.',
        image: coat2,
        name: 'coat2',
        price: 149.9
    },
    {
        id: 'c9e2768f-253d-4f2b-a2f6-9e0f4d8eb5b5',
        category: 'coat',
        description: 'Trendy coat for a chic and sophisticated look.',
        image: coat3,
        name: 'coat3',
        price: 99.9
    },
    {
        id: '462eb4b1-28c6-4f5d-b9b5-34dd1a3ef28d',
        category: 'shirt',
        description: 'Casual shirt for everyday comfort and style.',
        image: shirt1,
        name: 'shirt1',
        price: 49.9
    },
    {
        id: 'f0d1a2bf-2b94-48df-b8bb-ebefb82fc8b5',
        category: 'shirt',
        description: 'Versatile shirt that transitions effortlessly from day to night.',
        image: shirt2,
        name: 'shirt2',
        price: 59.9
    },
    {
        id: '0dab7674-df3a-4e4e-b0dc-6c6c0c1bc294',
        category: 'shirt',
        description: 'Stylish shirt with a modern twist, perfect for any occasion.',
        image: shirt3,
        name: 'shirt3',
        price: 39.9
    },
    {
        id: 'a9f55b80-14c3-4cf1-b32d-6a5317d0c0d1',
        category: 'pant',
        description: 'Comfortable pants that combine style and functionality.',
        image: pant1,
        name: 'pant1',
        price: 69.9
    },
    {
        id: '7db6b8f4-c32d-4870-9d2b-6d1bcb26e524',
        category: 'pant',
        description: 'Trendy pants to elevate your everyday wardrobe.',
        image: pant2,
        name: 'pant2',
        price: 79.9
    },
    {
        id: '2e981dd3-1e71-4c82-b9d7-99ff58d6a7b0',
        category: 'pant',
        description: 'Classic pants with a modern fit, suitable for any occasion.',
        image: pant3,
        name: 'pant3',
        price: 89.9
    },
    {
        id: '17a150d5-6b76-4b5b-ae2c-527c68ff788e',
        category: 'suit',
        description: 'Sophisticated suit for a polished and refined look.',
        image: suit1,
        name: 'suit1',
        price: 299.9
    },
    {
        id: '4e95dd9c-1535-4790-86eb-c6d0d2ffecb5',
        category: 'suit',
        description: 'Modern suit tailored for the contemporary gentleman.',
        image: suit2,
        name: 'suit2',
        price: 349.9
    },
    {
        id: 'b9308f81-9f99-4ed8-a326-0c4f39a07169',
        category: 'suit',
        description: 'Elegant suit crafted for special occasions and formal events.',
        image: suit3,
        name: 'suit3',
        price: 399.9
    },
];