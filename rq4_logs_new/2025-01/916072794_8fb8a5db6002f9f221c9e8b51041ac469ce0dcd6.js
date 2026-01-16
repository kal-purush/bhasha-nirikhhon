const apiUrl = "https://678504061ec630ca33a6cc51.mockapi.io/products";
const productContainer = document.getElementById('product-container');
const addProductButton = document.getElementById('add-product-button');
const addProductForm = document.getElementById('add-product-form');
const productForm = document.getElementById('product-form');

addProductButton.addEventListener('click', () => {
    addProductForm.style.display = 'block'; 
});

productForm.addEventListener('submit', async (e) => {
    e.preventDefault();

    const title = document.getElementById('title').value;
    const price = parseFloat(document.getElementById('price').value);
    const description = document.getElementById('description').value;
    const image = document.getElementById('image').value;

    const newProduct = {
        title,
        price,
        description,
        image
    };

    await createProduct(newProduct);

    addProductForm.style.display = 'none';

    productContainer.innerHTML = '';
    fetchProducts();
});

class Product {
    constructor(id, title, price, description, image) {
        this.id = id;
        this.title = title;
        this.price = price;
        this.description = description;
        this.image = image;
    }

    generateCard() {
        const card = document.createElement('div');
        card.classList.add('product-card');
        card.setAttribute('data-id', this.id);

        const img = document.createElement('img');
        img.src = this.image;
        img.alt = this.title;

        const title = document.createElement('h2');
        title.textContent = this.title;

        const price = document.createElement('p');
        price.classList.add('price');
        price.textContent = this.price ? `$${this.price.toFixed(2)}` : "Price unavailable";

        const description = document.createElement('p');
        description.classList.add('description');
        description.textContent = this.description;

        // زر التحديث
        const updateButton = document.createElement('button');
        updateButton.classList.add('update-button');
        updateButton.textContent = 'Update';
        updateButton.onclick = () => this.updateProduct();

        // زر الحذف
        const deleteButton = document.createElement('button');
        deleteButton.textContent = 'Delete';
        deleteButton.onclick = () => this.deleteProduct();

        card.appendChild(img);
        card.appendChild(title);
        card.appendChild(price);
        card.appendChild(description);
        card.appendChild(updateButton);
        card.appendChild(deleteButton);

        return card;
    }

    async updateProduct() {
        const newTitle = prompt("Enter new title for the product:", this.title);
        if (newTitle && newTitle !== this.title) {
            try {
                const response = await fetch(`${apiUrl}/${this.id}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ title: newTitle }),
                });
                const updatedProduct = await response.json();
                console.log('Updated Product:', updatedProduct);

                this.title = updatedProduct.title;
                const card = document.querySelector(`.product-card[data-id="${this.id}"]`);
                if (card) {
                    card.querySelector('h2').textContent = updatedProduct.title;
                }
            } catch (error) {
                console.error('Error updating product:', error);
            }
        }
    }

    async deleteProduct() {
        const confirmed = confirm(`Are you sure you want to delete the product: ${this.title}?`);
        if (confirmed) {
            try {
                const response = await fetch(`${apiUrl}/${this.id}`, {
                    method: 'DELETE',
                });
                if (response.ok) {
                    console.log(`Product with ID ${this.id} deleted`);

                    // إزالة المنتج من الواجهة بعد الحذف
                    const card = document.querySelector(`.product-card[data-id="${this.id}"]`);
                    if (card) {
                        card.remove();
                    }
                }
            } catch (error) {
                console.error('Error deleting product:', error);
            }
        }
    }
}

async function createProduct(newProduct) {
    try {
        const response = await fetch(apiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newProduct),
        });
        const createdProduct = await response.json();
        console.log('Created Product:', createdProduct);
    } catch (error) {
        console.error('Error creating product:', error);
    }
}

async function fetchProducts() {
    try {
        const response = await fetch(apiUrl);
        const data = await response.json();
        console.log(data);

        const products = data.slice(0, 20).map(item => new Product(item.id, item.title, item.price, item.description, item.image));

        products.forEach(product => {
            const card = product.generateCard();
            productContainer.appendChild(card);
        });
    } catch (error) {
        console.error('Error fetching products:', error);
    }
}

// تنفيذ العمليات
fetchProducts();