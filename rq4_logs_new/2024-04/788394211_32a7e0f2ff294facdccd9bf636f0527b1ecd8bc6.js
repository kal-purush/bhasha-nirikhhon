
// order.js
document.getElementById('orderForm').addEventListener('submit', function(event) {
    event.preventDefault();

    const formData = new FormData(this);
    const size = formData.get('size');
    const pepperoni = document.getElementById('pepperoni').checked;
    const extraCheese = document.getElementById('extraCheese').checked;

    axios.get('/pizza/order', {
        params: {
            size: size,
            pepperoni: pepperoni,
            extra_cheese: extraCheese
        }
    })
    .then(function (response) {
        document.getElementById('totalBill').innerText = 'Total Bill: RM ' + response.data.total;
    })
    .catch(function (error) {
        console.error('Error:', error);
    });

    // Reset the form after submission
    this.reset();
});