var product_1 = readline();
var product_2 = readline();

const productInfo_1 = product_1.split(" ").map((num) => + num);
const productInfo_2 = product_2.split(" ").map((num) => + num);

var quantity_P1 = productInfo_1[1];
var price_P1 = productInfo_1[2];

var quantity_P2 = productInfo_2[1];
var price_P2 = productInfo_2[2];

var total = ((quantity_P1 * price_P1) + (quantity_P2 * price_P2)).toFixed(2)

print("VALOR A PAGAR: " + "R$ " + total)

