
//array filter
/* const numbers=[10, 12, 13, 15 ,20 ,25];
const bigNumbers = numbers.filter(n => n>21);// numbers.filter(number => number>21);
console.log(bigNumbers);
 */
// object filter
/* const products = [
    { name: 'water bottle', price: 50, color: 'yellow' },
    { name: 'mobile phone', price: 15000, color: 'black' },
    { name: 'smart watch', price: 3000, color: 'black' },
    { name: 'sticky note', price: 30, color: 'pink' },
    { name: 'water glass', price: 3, color: 'white' }
];
const productPrices = products.filter(pd=>pd.price>500);
//products.filter(product=>product.price>500);
console.log(productPrices); */

//string filter

const products = [
    { name: 'water bottle', price: 50, color: 'yellow' },
    { name: 'mobile phone', price: 15000, color: 'black' },
    { name: 'smart watch', price: 3000, color: 'black' },
    { name: 'sticky note', price: 30, color: 'pink' },
    { name: 'water glass', price: 3, color: 'white' }
];
const productColor = products.filter(pd=>pd.color=='black');//products.filter(product=>product.color=='black');
console.log(productColor);