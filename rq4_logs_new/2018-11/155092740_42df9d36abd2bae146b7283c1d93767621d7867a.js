const { transaction } = require('objection');
const Order = require('../models/Order');

const getAllOrders = async(req, res, next) => {
    const  {tenantId}  = req.query;
    const orders = await Order.query()
    //.where(tenantId )
    .context({ 
        onBuild: builder => { 
            if (Order.isTenantSpecific){
                builder.where({tenantId})}
        }
    })
    .skipUndefined()
    
    res.send(orders);
}

module.exports = {getAllOrders}