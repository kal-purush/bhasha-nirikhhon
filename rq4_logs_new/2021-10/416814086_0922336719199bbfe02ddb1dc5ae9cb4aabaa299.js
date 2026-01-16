import CartNoticePage from "../../support/page/CartNoticePage";
import CategoryPage from "../../support/page/CategoryPage";
import ItemDetailPage from "../../support/page/ItemDetailPage";
import ItemListPage from "../../support/page/ItemListPage";
import MyShoppingBagPage from "../../support/page/MyShoppingBagPage";


describe('My Shopping Bag Page', () => {
    const category = new CategoryPage();
    const shoppingbag = new MyShoppingBagPage();
    const itemlist = new ItemListPage();
    const itemdetail = new ItemDetailPage();
    const cartnotice = new CartNoticePage();
    before(() => {
    })

    beforeEach(() => {
        cy.visit('https://www.pomelofashion.com/th/en/')
        // cy.clearCookies()
        // cy.clearLocalStorage()
        category.goToCategoryPage()
        category.clickAllDressesCategory()
        itemlist.clickFirstItemInPage()
        itemdetail.selectSize()
        itemdetail.clickAddToBagButton()
        cartnotice.clickCloseCartNotice()
        shoppingbag.clickCartIcon()
    })

    it('able to adjusts quantity of product items', () => {
        const quantity = '2'
        shoppingbag.adjustQuantity(quantity)
        shoppingbag.getItemQuantitySelected().should('have.text', quantity)
    })
    it('able to adjusts size of product items', () => {
        const size = 'XL'
        shoppingbag.adjustSize(size)
        shoppingbag.getItemSizeSelected().should('have.text', size)
    })
    it('able to delete product items', async () => {
        cy.wait(2000)
        // cy.get('.cart-product .cart-remove').click({ multiple: true })
        await shoppingbag.deleteItems()
        cy.wait(2000)
        shoppingbag.getProductInCart().should('not.exist')
    })
    it('able to fill-in and click apply promo code', () => {
        const promocode = 'abc'
        shoppingbag.fillInPromotionCode(promocode)
        shoppingbag.clickApplyButton()
        cy.contains('Invalid voucher code')
    })
    it('able to proceed to checkout', () => {
        shoppingbag.clickProceedToCheckoutButton()
    })



})