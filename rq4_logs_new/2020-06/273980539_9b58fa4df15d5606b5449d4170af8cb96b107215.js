const Product = require('../models/Product');

module.exports = {
    async index(req, res) {
        const products = await Product.findAll();

        return res.render('cardapio', {
            products
        });
    },
}