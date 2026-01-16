//TODO : Fix cartBtn
// const cartBtn = document.querySelector('.cart-btn');
const cartModal = document.querySelector('.cart');
const backDrop = document.querySelector('.backdrop');
const closeModal = document.querySelector('.cart-item-confirm');

import { productsData } from './products';

// get products
class Products {
  //
  getProduct() {
    return productsData;
  }
}

// display products
class UI {
  //
}

// Storage
class Storage {
  //
}

document.addEventListener('DOMContentLoaded', () => {
  console.log('loaded');
});

// Cart item Functions
function showModalFunction() {
  backDrop.style.display = 'block';
  cartModal.style.opacity = '1';
  cartModal.style.top = '20%';
}

function closeModalFunction() {
  backDrop.style.display = 'none';
  cartModal.style.opacity = '0';
  cartModal.style.top = '-100%';
}

cartBtn.addEventListener('click', showModalFunction);
closeModal.addEventListener('click', closeModalFunction);
backDrop.addEventListener('click', closeModalFunction);