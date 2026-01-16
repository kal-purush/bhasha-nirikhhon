const d = document,
    openCart = d.querySelector('.menu-cart'),
    cartBlock = d.querySelector('.modul-cart'),
    cartTop = d.querySelector('.cart-text__top'),
    productCountChange = d.querySelectorAll('.count-plus, .count-minus'),
    makeOrderButtons = d.querySelectorAll('.btn'),
    makeOrderButtonsMob = d.querySelectorAll('.btn-mob'),
    totalCountBlock = openCart.querySelector('.cart-count'),
    totalSumBlock = cartBlock.querySelector('.total-num'),
    clearCart = cartBlock.querySelector('.cart-clear'),
    cartTitle = cartBlock.querySelector('.cart-title'),
    priceModal = d.querySelector('.modal-price'),
    makeOrder = d.querySelector('.cart-btn');

let productCountBlock,
    productTitle,
    productImage,
    productSpec,
    productPrise;

const cart = [];

// TODO This should be done in CSS.
openCart.style.cursor = 'pointer';
clearCart.style.cursor = 'pointer';
makeOrder.style.cursor = 'pointer';
cartBlock.style.overflow = 'auto';

makeOrder.addEventListener('click', () => {
    let sendFormButton = d.querySelector('.modal-wrap .main-btn-border .btn-click');
    if (cart.length > 0) {
        priceModal.textContent = `Сумма заказа: ${totalSum()} руб`;
        sendFormButton.removeAttribute('disabled');
    } else {
        priceModal.textContent = `Ваша корзина пуста...`;
        sendFormButton.setAttribute('disabled', '');
    }
});

d.querySelectorAll('.count-text')
    .forEach((item) => item.textContent = '1');

openCart.addEventListener('click', function (e) {
    let target = e.target;
    if (target.classList.contains('cart-count') || target.tagName === 'IMG') {
        e.target = this;
    }

    cartBlock.style.display === 'block' ? cartBlock.style.display = 'none' : cartBlock.style.display = 'block';
});


let bod = d.querySelector('body'),
    wrap = d.querySelector('.container60');
soup = d.querySelector('.soup'),
    salat = d.querySelector('.salat'),
    drinks = d.querySelector('.drinks'),
    home = d.querySelector('#home');
console.log(home);
bod.addEventListener('click', function (e) {

    let target = e.target;
    console.log(target.parentNode );
    if (target.parentNode === this || target.parentNode === wrap ||target.parentNode ===  soup ||target.parentNode ===  salat||target.parentNode ===  drinks ||target.parentNode ===  home) {
        console.log(1);
        cartBlock.style.display = 'none'
    }

});


const arrayObjectIndexOf = (arr, key, param) => arr.findIndex(el => el[key] === param);

productCountChange.forEach((item) => {
    item.addEventListener('click', function () {
        getCardElements(this);
        let val = 1,
            count = +productCountBlock.textContent;
        if (this.classList.contains('count-minus')) {
            val = -1;
        }
        count += val;
        if (count >= 1) {
            productCountBlock.textContent = count;
        }
        if (count > 1) {
            item.parentNode.childNodes[1].style.backgroundColor = 'black'
        } else if (count < 2) {
            item.parentNode.childNodes[1].style.backgroundColor = '#c7c7c7'

        }


    });
});

makeOrderButtons.forEach((btn) => {
    btn.addEventListener('click', function () {
        getCardElements(this);

        const volume = productSpec.textContent.split('/')[1].trim().slice(0, -1);
        const img = productImage.getAttribute('src');
        const price = +productPrise.textContent.split(' ')[0];

        const product = {
            title: productTitle.textContent,
            img: img,
            spec: volume,
            price: price,
            count: +productCountBlock.textContent
        };

        const index = arrayObjectIndexOf(cart, 'title', product.title);
        if (index === -1) {
            cart.push(product);
        } else {
            cart[index].count += product.count;
        }
        renderFullCart();
    })
});
makeOrderButtonsMob.forEach((btn) => {
    btn.addEventListener('click', function () {
        getCardElements(this);

        const volume = productSpec.textContent.split('/')[1].trim().slice(0, -1);
        const img = productImage.getAttribute('src');
        const price = +productPrise.textContent.split(' ')[0];

        const product = {
            title: productTitle.textContent,
            img: img,
            spec: volume,
            price: price,
            count: +productCountBlock.textContent
        };

        const index = arrayObjectIndexOf(cart, 'title', product.title);
        if (index === -1) {
            cart.push(product);
        } else {
            cart[index].count += product.count;
        }
        renderFullCart();
    })
});

const totalCount = () => cart.length;

const totalSum = () => {
    let cost = 0;
    cart.forEach((item) => {
        cost += (item.count * item.price);
    });
    return cost;
};

const renderFullCart = () => {
    d.querySelectorAll('.cart-product')
        .forEach((cartProduct) => {
            cartProduct.remove();
        });

    cartTitle.textContent = `Корзина (${totalCount()})`;
    totalCountBlock.textContent = totalCount();
    totalSumBlock.textContent = totalSum() + ' ₽';

    cart.forEach((cartItem, key) => {
        renderCartProduct(cartItem, key);
    });

    cartBlock.querySelectorAll('.cart-count-pulse, .cart-count-minuse')
        .forEach((addBtn) => {

            addBtn.addEventListener('click', function () {
                const index = +this.dataset.index;
                let count = cart[index].count;
                const val = +this.dataset.val;

                if (count > 0) {
                    if (count === 1 && val === -1) {
                        return;
                    }
                    cart[index].count += val;
                }

                renderFullCart();
            });
        });

    cartBlock.querySelectorAll('.btn-close')
        .forEach((delBtn) => {
            delBtn.addEventListener('click', function () {
                const index = +this.dataset.index;
                cart.splice(index, 1);
                renderFullCart();
            })
        })
};

clearCart.addEventListener('click', () => {
    cart.length = 0;
    renderFullCart();
});

const getCardElements = (elem) => {
    const card = elem.closest('.card-product');
    productCountBlock = card.querySelector('.count-text');
    productTitle = card.querySelector('.card-product__title');
    productImage = card.querySelector('img');
    productSpec = card.querySelector('.card-product__specifications');
    productPrise = card.querySelector('.card-price');
};

const renderCartProduct = ({img, title, spec, price, count}, index) => {
    const cartProduct = d.createElement('div');
    cartProduct.classList.add('cart-product');
    cartProduct.innerHTML = `
    <div class="cart-product-main">
        <div class="cart-product-image">
            <img class="prod-img" src="${img}" alt="img">
        </div>
        <div class="cart-product-title">
            ${title}
            <div class="cart-product-title_sub">Порция ${spec}</div>
        </div>
        
        </div>
            <div class="cart-product-count">
                <div class="cart-count">
                    <div class="cart-count-minuse" data-index="${index}" data-val="-1">-</div>
                    <div class="cart-count-number">${count}</div>
                    <div class="cart-count-pulse" data-index="${index}" data-val="1">+</div>
                </div>
                <div class="cart-price">${price} ₽</div>
            </div>
        <div class="btn-close" data-index="${index}">
    </div>`;

    cartTop.insertAdjacentElement('afterend', cartProduct);
};
let html = document.querySelector('html');

document.addEventListener('click', () => {
    if (cartBlock.style.display === 'block') {

        html.style.overflowY = 'hidden'
    } else {

        html.style.overflowY = 'scroll'
    }
});