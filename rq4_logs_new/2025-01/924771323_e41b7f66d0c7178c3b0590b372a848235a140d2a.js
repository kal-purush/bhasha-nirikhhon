let products = JSON.parse(localStorage.getItem("products")) || [];
let bids = JSON.parse(localStorage.getItem("bids")) || {};

function addProduct() {
    const name = document.getElementById("product-name").value.trim();
    const price = Number(document.getElementById("starting-price").value);
    const image = document.getElementById("product-image").value.trim();

    if (!name || price <= 0 || !image) {
        alert("Please enter a valid product name, price, and image URL.");
        return;
    }

    const product = { id: products.length + 1, name, price, image };
    products.push(product);
    saveData();
    displayProducts();

    // Clear input fields
    document.getElementById("product-name").value = "";
    document.getElementById("starting-price").value = "";
    document.getElementById("product-image").value = "";
}

function saveData() {
    localStorage.setItem("products", JSON.stringify(products));
    localStorage.setItem("bids", JSON.stringify(bids));
    alert("Data saved successfully!");
}

function resetAuction() {
    localStorage.clear();
    products = [];
    bids = {};
    displayProducts();
    displayBidders();
}

function displayProducts() {
    const productList = document.getElementById("product-list");
    productList.innerHTML = "";
    products.forEach(product => {
        productList.innerHTML += `
            <div>
                <h3>${product.name}</h3>
                <p>Starting Price: ₹${product.price}</p>
                <img src="${product.image}" width="100">
                <button onclick="openBidSection(${product.id}, '${product.name}', '${product.image}')">Bid Now</button>
            </div>`;
    });
}

function displayBidders() {
    const topBidders = document.getElementById("top-bidders");
    topBidders.innerHTML = "";
    Object.keys(bids).forEach(productId => {
        const productBids = bids[productId].bidders;
        const topBidder = productBids.reduce((max, bidder) => bidder.amount > max.amount ? bidder : max, { amount: 0 });
        topBidders.innerHTML += `<p>Top Bidder for ${products.find(p => p.id == productId).name}: ${topBidder.name} - ₹${topBidder.amount}</p>`;
    });
}

function openBidSection(productId, productName, productImage) {
    document.getElementById("bid-section").style.display = "block";
    document.getElementById("bid-product-name").textContent = productName;
    document.getElementById("bid-product-image").src = productImage;
    document.getElementById("bidder-name").value = "";
    document.getElementById("bid-amount").value = "";
    document.getElementById("bidder-list").innerHTML = "";
    displayBiddersForProduct(productId);

    document.getElementById("bid-section").onsubmit = function(event) {
        event.preventDefault();
        placeBid(productId);
    };
}

function placeBid(productId) {
    const name = document.getElementById("bidder-name").value.trim();
    const amount = Number(document.getElementById("bid-amount").value);

    if (!name || amount <= 0) {
        alert("Please enter a valid bid.");
        return;
    }

    if (!bids[productId]) {
        bids[productId] = { bidders: [] };
    }

    bids[productId].bidders.push({ name, amount });
    saveData();
    displayBiddersForProduct(productId);
    displayBidders();
}

function displayBiddersForProduct(productId) {
    const bidderList = document.getElementById("bidder-list");
    bidderList.innerHTML = "";
    if (bids[productId]) {
        bids[productId].bidders.forEach(bidder => {
            const li = document.createElement("li");
            li.textContent = `${bidder.name}: ₹${bidder.amount}`;
            bidderList.appendChild(li);
        });
    }
}

function closeBidSection() {
    document.getElementById("bid-section").style.display = "none";
}

// Load existing data
displayProducts();
displayBidders();