async function fetchProducts() {
    const response = await fetch('api/products', {
        method: 'GET',
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    });
    return await response.json();
}

function createProductHTML(product) {
    return `<tr>
            <td>${product.name}</td>
            <td>${product.price}</td>
            <td>${product.stock}</td>
            <td>
              <button type="button" class="btn btn-outline-info" onclick="updateProduct('${product.idProduct}')">Update</button>
              <button type="button" class="btn btn-outline-danger" onclick="deleteProduct('${product.idProduct}')">Remove</button>
            </td>
          </tr>`;
}

function updateProduct(productId) {
    window.location.href = `./edit.html?id=${productId}`;
}

function renderProducts(products) {
    document.querySelector("#products tbody").innerHTML = products.map(createProductHTML).join('');
}

async function chargeUsers() {
    try {
        const products = await fetchProducts();
        renderProducts(products);
    } catch (error) {
        console.error('Error loading products:', error);
    }
}

async function deleteProduct(id){
    if (confirm('Delete product?')){
        const request = await fetch('api/products/'+ id, {
            method: 'DELETE',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        });
        location.reload();
    }
}

chargeUsers().then(r => {});