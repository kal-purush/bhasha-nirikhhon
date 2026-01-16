const Shop = require('../schema/shop');

const createShop = new Shop({
  name: "peach",
  price: 70,
  amount: 8
})

const createCommodity = createShop.save(function(err){
  if(err) throw err;
  console.log('Commodity created!!!');
})

module.exports = createCommodity;