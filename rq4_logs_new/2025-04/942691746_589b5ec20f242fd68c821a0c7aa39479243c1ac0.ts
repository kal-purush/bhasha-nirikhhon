import { Basket } from '../Types/types';

export const baskets: Basket[] = [];

export function initializeBaskets(): void {
    // Clear existing baskets
    console.log("Initializing baskets..."); 
    baskets.length = 0;
    
    // Create 3 baskets with fixed positions
    const basketPositions = [
        { id: 'basket1', left: 40, top: 72 },
        { id: 'basket2', left: 50, top: 72 },
        { id: 'basket3', left: 60, top: 72 }
    ];
    
    basketPositions.forEach(pos => {
        baskets.push({
            id: pos.id,
            left: pos.left,
            top: pos.top,
            position: { left: pos.left, top: pos.top }
        });
    });

    // Create DOM elements for each basket
    baskets.forEach(createBasketElement);
}

function createBasketElement(basket: Basket): void {
    const basketElement = document.createElement('img');
    basketElement.id = basket.id;
    basketElement.classList.add('basket');
    basketElement.src = "../assets/rotrung.png";
    basketElement.style.position = 'absolute';
    basketElement.style.width = '100px';
    basketElement.style.left = `${basket.left}%`;
    basketElement.style.top = `${basket.top}%`;
    basketElement.style.zIndex = '1';
    
    document.body.appendChild(basketElement);
}