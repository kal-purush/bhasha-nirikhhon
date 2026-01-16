const Joi = require('joi')
const productsFilter = Joi.object().keys({
    category:Joi.string().optional(),
    recommendation:Joi.string().valid('all', 'recommend', 'notRecommend').optional(),
    search:Joi.string().optional()
})
module.exports = {productsFilter}