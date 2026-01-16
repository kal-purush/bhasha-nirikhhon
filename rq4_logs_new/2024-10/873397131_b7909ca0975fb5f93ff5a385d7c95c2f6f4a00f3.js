const products = [
            { id: 1, name: 'Shoes', price: 500, imageUrl: 'https://tse1.mm.bing.net/th?id=OIP.LAQdUJeZNakfd27ikMynogHaFP&pid=Api&P=0&h=180' },
            { id: 2, name: 'Smart phone', price: 20000, imageUrl: 'https://tse4.mm.bing.net/th?id=OIP.MQcuDb0hMad1CKT66NMXJAHaHa&pid=Api&P=0&h=180' },
            { id: 3, name: 'Watch', price: 800, imageUrl: 'https://tse4.mm.bing.net/th?id=OIP.ZXNqhKE9zQ8yN1KsKm0OvwHaFE&pid=Api&P=0&h=180' },
            { id: 4, name: 'Bag', price: 600, imageUrl: 'https://tse4.mm.bing.net/th?id=OIP.ufx5Bj79kao4cDg9dOo--AHaHa&pid=Api&P=0&h=180' },
            { id: 5, name: 'laptop', price: 60000, imageUrl: 'https://tse2.mm.bing.net/th?id=OIP.Ir29KH8ifMH_oEOBLg6uYwHaHa&pid=Api&P=0&h=180' },
            { id: 6, name: 'ipad', price: 40000, imageUrl: 'https://tse2.mm.bing.net/th?id=OIP.4G4sv7rd1ZicElWfAIWKdAHaHa&pid=Api&P=0&h=180' },
            { id: 7, name: 'PS5', price: 90000, imageUrl: 'https://tse4.mm.bing.net/th?id=OIP.LaYgeT1iZB1nd80PHaQ9BwAAAA&pid=Api&P=0&h=180' },
            { id: 8, name: 'TV', price: 10000, imageUrl: 'https://tse2.mm.bing.net/th?id=OIP.S_jVrYBnmy2DbUpiuU7o3gHaE8&pid=Api&P=0&h=180' },
                ];

        const cart = [];

        function renderProducts() {
            const productList = document.getElementById('product-list');
            productList.innerHTML = '';

            products.forEach(product => {
                const productElement = document.createElement('div');
                productElement.className = 'product';
                productElement.innerHTML = `
                    <img src="${product.imageUrl}" alt="${product.name}" />
                    <h3>${product.name}</h3>
                    <p>Price: Rs.${product.price}</p>
                    <button onclick="addToCart(${product.id})">Add to Cart</button>
                `;
                productList.appendChild(productElement);
            });
        }

      
        function addToCart(productId) {
            const product = products.find(p => p.id === productId);
            const existingProduct = cart.find(item => item.id === productId);

            if (existingProduct) {
                existingProduct.quantity += 1;
            } else {
                cart.push({ ...product, quantity: 1 });
            }

            renderCart();
        }

        function removeFromCart(productId) {
            const productIndex = cart.findIndex(item => item.id === productId);
            if (productIndex > -1) {
                cart.splice(productIndex, 1);
            }
            renderCart();
        }

        function updateQuantity(productId, quantity) {
            const product = cart.find(item => item.id === productId);
            if (product) {
                product.quantity = quantity;
                if (product.quantity <= 0) {
                    removeFromCart(productId);
                } else {
                    renderCart();
                }
            }
        }

        function renderCart() {
            const cartItems = document.getElementById('cart-items');
            cartItems.innerHTML = '';
            let totalPrice = 0;

            cart.forEach(item => {
                const itemElement = document.createElement('li');
                itemElement.className = 'cart-item';
                itemElement.innerHTML = `
                    <img src="${item.imageUrl}" alt="${item.name}" />
                    ${item.name} - Rs.${item.price} x 
                    <input type="number" value="${item.quantity}" min="1" onchange="updateQuantity(${item.id}, this.value)" />
                    <button onclick="removeFromCart(${item.id})">Remove</button>
                `;
                cartItems.appendChild(itemElement);
                totalPrice += item.price * item.quantity;
            });

            document.getElementById('total-price').textContent = totalPrice.toFixed(2);
        }

        function showSection(sectionId) {
            document.getElementById('home').style.display = 'none';
            document.getElementById('products').style.display = 'none';
            document.getElementById('cart').style.display = 'none';
            document.getElementById(sectionId).style.display = 'block';
        }

        renderProducts();