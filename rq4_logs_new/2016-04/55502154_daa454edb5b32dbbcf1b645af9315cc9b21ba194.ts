import {Pizza} from './pizza';

export var PIZZAS: Pizza[] = [
  {
    name: '4 STAGIONI',
    price: 27.5,
    weight: 550,
    image: 'images/quattro.png',
    ingredients: 'sos rosii, mozzarella, ciuperci, salam, sunca presata, oregano, anghinare',
    canPurchase: true,
    soldOut: false
  },
  {
    name: 'PEPPERONI',
    price: 25,
    weight: 450,
    image: 'images/pepperoni.png',
    ingredients: 'sos rosii, mozzarella, salam picant, oregano',
    canPurchase: false,
    soldOut: false
  }
];