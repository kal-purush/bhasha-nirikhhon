module.exports = function (temp, product){
    let output = temp.replace(/{%PRODUCT_NAME%}/g, product.productName);
    output = temp.replace(/{%PRODUCT_IMAGE%}/g, product.image);
    output = temp.replace(/{%PRODUCT_PRICE%}/g, product.price);
    output = temp.replace(/{%PRODUCT_FROM%}/g, product.from);
    output = temp.replace(/{%PRODUCT_NUTRIMENTS%}/g, product.nutriments);
    output = temp.replace(/{%PRODUCT_QUANTITY%}/g, product.quantity);
    output = temp.replace(/{%PRODUCT_DESCRIPTION%}/g, product.description);

    if(!product.organic){
        output = output.replace(/{%NOT_ORGANIC%}/g, )
    }

    return output;
}