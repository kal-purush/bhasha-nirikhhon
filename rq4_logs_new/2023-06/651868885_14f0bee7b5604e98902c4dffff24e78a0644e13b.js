  // Get references to the input fields
  const productSelect = document.getElementById('product');
  const quantityInput = document.getElementById('quantity');
  const totalCostInput = document.getElementById('totalCost');

  // Add an event listener to detect changes in the inputs
  productSelect.addEventListener('change', updateTotalCost);
  quantityInput.addEventListener('input', updateTotalCost);

  function updateTotalCost() {
    // Get the selected product and quantity entered by the user
    const selectedProduct = productSelect.value;
    const quantity = parseFloat(quantityInput.value);

    // Define the unit cost based on the selected product
    let unitCost;
    if (selectedProduct === 'beans') {
      unitCost = 5.99; // Sample unit cost for beans
    } else if (selectedProduct === 'rice') {
      unitCost = 3.99; // Sample unit cost for rice
    } else if (selectedProduct === 'wheat') {
      unitCost = 4.99; // Sample unit cost for wheat
    } else {
      unitCost = 0; // If no product is selected, set the unit cost to 0
    }

    // Perform the multiplication and update the total cost field
    const totalCost = unitCost * quantity;
    totalCostInput.value = isNaN(totalCost) ? '' : totalCost.toFixed(2);
  }