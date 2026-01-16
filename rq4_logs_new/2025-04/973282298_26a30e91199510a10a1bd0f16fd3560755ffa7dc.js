// API base URL
const API_URL = 'http://localhost:3000/api';

// DOM Elements
const productList = document.getElementById('productList');
const productForm = document.getElementById('productForm');
const formTitle = document.getElementById('formTitle');
const productName = document.getElementById('productName');
const productDescription = document.getElementById('productDescription');
const productPrice = document.getElementById('productPrice');
const saveButton = document.getElementById('saveButton');
const cancelButton = document.getElementById('cancelButton');

// State
let editingProductId = null;

// Event Listeners
document.addEventListener('DOMContentLoaded', loadProducts);
productForm.addEventListener('submit', handleFormSubmit);
cancelButton.addEventListener('click', resetForm);

// Functions
async function loadProducts() {
    try {
        const response = await fetch(`${API_URL}/products`);
        if (!response.ok) {
            throw new Error('Failed to fetch products');
        }
        const products = await response.json();
        displayProducts(products);
    } catch (error) {
        console.error('Error loading products:', error);
        alert('Failed to load products. Please make sure the backend server is running.');
    }
}

function displayProducts(products) {
    productList.innerHTML = '';
    products.forEach(product => {
        const listItem = document.createElement('li');
        listItem.className = 'list-group-item';
        listItem.innerHTML = `
            <div class="product-info">
                <h5 class="mb-1">${product.name}</h5>
                <p class="mb-1">${product.description}</p>
                <span class="product-price">$${product.price}</span>
            </div>
            <div class="product-actions">
                <button class="btn btn-sm btn-primary" onclick="editProduct(${product.id})">Edit</button>
                <button class="btn btn-sm btn-danger" onclick="deleteProduct(${product.id})">Delete</button>
            </div>
        `;
        productList.appendChild(listItem);
    });
}

async function handleFormSubmit(e) {
    e.preventDefault();
    
    const productData = {
        name: productName.value,
        description: productDescription.value,
        price: parseFloat(productPrice.value)
    };

    try {
        if (editingProductId) {
            await updateProduct(editingProductId, productData);
        } else {
            await createProduct(productData);
        }
        resetForm();
        loadProducts();
    } catch (error) {
        console.error('Error saving product:', error);
        alert('Failed to save product. Please try again.');
    }
}

async function createProduct(productData) {
    const response = await fetch(`${API_URL}/products`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        },
        body: JSON.stringify({ product: productData })
    });
    
    if (!response.ok) {
        throw new Error('Failed to create product');
    }
}

async function updateProduct(id, productData) {
    const response = await fetch(`${API_URL}/products/${id}`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        },
        body: JSON.stringify({ product: productData })
    });
    
    if (!response.ok) {
        throw new Error('Failed to update product');
    }
}

async function deleteProduct(id) {
    if (!confirm('Are you sure you want to delete this product?')) {
        return;
    }

    try {
        const response = await fetch(`${API_URL}/products/${id}`, {
            method: 'DELETE',
            headers: {
                'Accept': 'application/json'
            }
        });
        
        if (!response.ok) {
            throw new Error('Failed to delete product');
        }
        
        loadProducts();
    } catch (error) {
        console.error('Error deleting product:', error);
        alert('Failed to delete product. Please try again.');
    }
}

function editProduct(id) {
    editingProductId = id;
    formTitle.textContent = 'Edit Product';
    
    fetch(`${API_URL}/products/${id}`, {
        headers: {
            'Accept': 'application/json'
        }
    })
        .then(response => {
            if (!response.ok) {
                throw new Error('Failed to fetch product details');
            }
            return response.json();
        })
        .then(product => {
            productName.value = product.name;
            productDescription.value = product.description;
            productPrice.value = product.price;
        })
        .catch(error => {
            console.error('Error loading product details:', error);
            alert('Failed to load product details. Please try again.');
        });
}

function resetForm() {
    editingProductId = null;
    formTitle.textContent = 'Add New Product';
    productForm.reset();
} 