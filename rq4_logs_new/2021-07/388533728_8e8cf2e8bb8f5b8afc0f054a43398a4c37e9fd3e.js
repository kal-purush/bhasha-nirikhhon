import('./product-card.js')

class HeaderContainer extends WebComponent {
    get css() { return `
		<style>
		    .header {
		        display: flex;
		        flex-direction: column;
		        align-items: center;
		    }
		    
		    .header__content, .header__categories {
		        width: 95%;
		        max-width: 1440px;
		    }
		    
		    .header__content {
		        padding: 20px 10px;
		    }
		    
		    .category-button-container, .logo, .basket-search-container {
		        width: 300px;
		    }
		    
		    .header__content {
		        display: flex;
		        justify-content: space-between;
		        align-items: center;
		    }
		    
		    .category-button-container {
		        height: 40px;
		        display: flex;
		        align-items: center;
		    }
		    
		    .category-button {
		        width: 33px;
		        height: 33px;
		        cursor: pointer;
		        position: absolute;
		        opacity: 0.8;
		        transition: all 0.2s ease-out;
		    }
		    
		    .category-button:hover {
		        opacity: 1;
		    }
		    
		    .button__top-line, .button__bottom-line {
		        height: 2px;
		        width: 40px;
		        background-color: azure;
		        border-radius: 1px 1px 1px 1px;
		        position: relative;
		    }
		    
		    .button__top-line {
		        transform: rotate(45deg);
		        top: 16px;
		        left: -3px;
		    }
		    
		    .button__bottom-line {
		        transform: rotate(-45deg);
		        top: 14px;
		        left: -3px;
		    }
		    
            .logo {
                color: azure;
                margin: 0;
                text-align: center;
                font: 40px "Circe Bold";
            }
            
            .basket-search-container, .basket-container, .search-container {
                display: flex;
                align-items: center;
            }
            
            .basket-search-container {
                justify-content: space-between;
            }
            
            .basket-icon {
                width: 30px;
                height: 30px;
                color: azure;
                background-image: url('/img/icons/basket-icon.png');
            }
            
		</style>
	`}

    get html() { return `
        <header class="header">
            <div class="header__content">    
                <div class="category-button-container">
                    <div class="category-button">
                        <div class="button__top-line"></div>
                        <div class="button__bottom-line"></div>
                    </div>
                </div>
                <h1 class="logo"></h1>
                <div class="basket-search-container">
                    <div class="basket-container">
                        <span class="basket-counter"></span>
                        <div class="basket-icon"></div>
                    </div>
                    <form class="search-container">
                        <input type="text" class="search-bar" placeholder="Поиск...">
                        <div class="search-icon">
                        <button class="search-button"></button>
                        </div>
                    </form>
                </div>
                
            </div>
            <div class="header__categories">
                <div class="tst-div"></div>
            </div>
        </header>
	`}

    constructor() {
        super()
        this.amount = 0
    }

    async connectedCallback() {
        // const basketIcon = this.shadowRoot.querySelector('.basket-icon')
        const logo = this.shadowRoot.querySelector('.logo')

        // basketIcon.style.backgroundImage = 'url(img/icons/basket-icon.png);'
        logo.innerText = 'COSE RUBATE'
    }


    // addListeners() {
    //     this.addEventListener('add_product_to_cart', ({detail}) => {
    //         const idx = window.shop.products.findIndex(el => el.id === +detail)
    //         this.calculateAmount(window.shop.products[idx])
    //     })
    // }
    //
    // calculateAmount(product) {
    //     const price = product.price
    //     if (product.discount) {
    //         const priceDiscount = (product.price * (100 - product.discount)/100)
    //         this.amount += priceDiscount
    //     } else {
    //         this.amount += price
    //     }
    //     this.renderAmount()
    // }
    //
    // renderAmount() {
    //     this.shadowRoot.querySelector('.amount').innerText = this.amount
    // }
}

customElements.define('header-container', HeaderContainer)