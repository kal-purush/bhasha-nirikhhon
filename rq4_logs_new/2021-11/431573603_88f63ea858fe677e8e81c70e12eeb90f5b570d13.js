export function calculateCartTotal(products, cart, discountAmount = 0) {
    let total = 0;
    for (let id in products) {
      const { price } = products[id];
      const quantity = cart[id] || 0;
      total += price * quantity;
    }
    let totalWithDiscount = total * (1 - discountAmount);
    console.log(`discount amount is `, discountAmount)
    return totalWithDiscount.toFixed(2);
  }

  export function calculateTotalQuantity(cart) {
    let totalQuantity = 0;
    for (let id in cart) {
      totalQuantity += cart[id];
    }
    return totalQuantity;
  }