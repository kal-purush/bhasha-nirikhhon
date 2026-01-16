///  <reference types="cypress"/>

class dress{

dresses(){
const dr = cy.xpath("//div[@id='subcategories']//li[2]//div[1]//a[1]//img[1]")
dr.click()
return this;
}

evening(){
const ev= cy.xpath("//div[@id='subcategories']//li[2]//div[1]//a[1]//img[1]")
ev.click()
return this;    
}

View(){
const quick = cy.xpath("//a[@class='product_img_link']//img[@class='replace-2x img-responsive']")
quick.trigger('mousedown', {which: 1})
quick.xpath("//a[@class='quick-view']").click({force: true})
return this;
}


Popup(){
cy.window().then((win) => {
cy.xpath("//span[contains(text(),'Add to cart')]").click({force: true})
})
return this;
}

shopping(){
 const conti = cy.xpath("//div[@id='layer_cart']//div[@class='clearfix']")  
 conti.wait(2000)    
conti.xpath("/html[1]/body[1]/div[1]/div[1]/header[1]/div[3]/div[1]/div[1]/div[4]/div[1]/div[2]/div[4]/span[1]/span[1]")
conti.click({force: true})
conti.wait(2000)
return this;
}



}

export default dress