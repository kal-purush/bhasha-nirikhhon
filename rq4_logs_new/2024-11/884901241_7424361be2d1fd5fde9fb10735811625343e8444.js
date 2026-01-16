// stores/cartStore.js
import { defineStore } from 'pinia';

export const useCartStore = defineStore('cart', {
    state: () => ({
        cartItems: [],
    }),
    getters: {
        subtotal(state) {
            return state.cartItems.reduce((sum, item) => sum + (item.price * item.quantity), 0);
        },
        total(state) {
            return state.subtotal + 499; // Shipping cost
        },
    },
    actions: {
        addToCart(product) {
            const existingProduct = this.cartItems.find((item) => item.id === product.id);
            if (existingProduct) {
                // Si el producto ya existe, incrementamos su cantidad
                existingProduct.quantity++;
            } else {
                // Si no existe, lo agregamos al carrito
                this.cartItems.push({ ...product, quantity: 1 });
            }
        },
        increaseQuantity(index) {
            // Aseguramos que la cantidad nunca sea menor que 1
            if (this.cartItems[index].quantity < 999) {
                this.cartItems[index].quantity++;
            }
        },
        decreaseQuantity(index) {
            // Solo decrementamos si la cantidad es mayor que 1
            if (this.cartItems[index].quantity > 1) {
                this.cartItems[index].quantity--;
            }
        },
        removeItem(index) {
            // Elimina el producto del carrito por su índice
            this.cartItems.splice(index, 1);
        },
    },
});