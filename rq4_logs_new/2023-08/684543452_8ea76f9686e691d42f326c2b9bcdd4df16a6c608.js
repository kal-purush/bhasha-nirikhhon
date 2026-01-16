const items = document.querySelectorAll('.item');
const totalPriceElement = document.getElementById('total-price');

items.forEach(item => {
    const quantityElement = item.querySelector('.quantity');
    const minusButton = item.querySelector('.minus');
    const plusButton = item.querySelector('.plus');
    const likeButton = item.querySelector('.like-btn');
    const deleteButton = item.querySelector('.delete-btn');
    const priceElement = item.querySelector('.price');
    let quantity = parseInt(quantityElement.textContent);
    const price = parseFloat(priceElement.textContent.slice(1)); // Remove the dollar sign

    minusButton.addEventListener('click', () => {
        if (quantity > 1) {
            quantity--;
            quantityElement.textContent = quantity;
            updateTotalPrice();
        }
    });

    plusButton.addEventListener('click', () => {
        quantity++;
        quantityElement.textContent = quantity;
        updateTotalPrice();
    });

    likeButton.addEventListener('click', () => {
        likeButton.classList.toggle('liked');
    });

    deleteButton.addEventListener('click', () => {
        item.remove();
        updateTotalPrice();
    });

    function updateTotalPrice() {
        const totalPrice = [...items].reduce((total, item) => {
            const itemQuantity = parseInt(item.querySelector('.quantity').textContent);
            const itemPrice = parseFloat(item.querySelector('.price').textContent.slice(1));
            return total + itemQuantity * itemPrice;
        }, 0);

        totalPriceElement.textContent = `$${totalPrice.toFixed(2)}`;
    }
});