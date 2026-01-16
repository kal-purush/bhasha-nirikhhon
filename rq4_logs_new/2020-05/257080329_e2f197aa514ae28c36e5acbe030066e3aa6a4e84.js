const userController = require('../controllers/user');
const OrderStatusController = require('../controllers/order');
const rolController = require('../controllers/rol');
const express = require('express');
const jwt = require('jsonwebtoken');
const jwtSign = "mytokenpassword";
const router_order_status = express.Router();
const ROLE_ADMIN_DESCRIPTION = "Administrator";

router_order_status.post('/createstatus', validateToken, validateUserRol, validateStatusProps, async ( req, res ) => {
    let saveOrderStatus = await OrderStatusController.insertOrderStatus( req.body );

    if( saveOrderStatus ) {
        res.statusCode = 200;  
        return res.json("order status added sucessfully");
    }
     
});

router_order_status.get('/status', validateToken, validateUserRol, async ( req, res ) => {
    let ordersStatus = await OrderStatusController.getOrdersStatus();

    res.statusCode = 200;
    res.json( ordersStatus );
});

router_order_status.get('/status/:id', validateToken, validateUserRol, async ( req, res ) => {
    const statusId = req.params.id;
    let status = await OrderStatusController.getOrderStatusBy( statusId )
    .then( result => result );

    res.statusCode = 200;
    return res.json(status);
    
});

router_order_status.delete('/status/:id', validateToken, validateUserRol, async( req, res ) => {
    const statusId = req.params.id;

    let deleteStatus = await OrderStatusController.deleteOrderStatus( statusId );

    if( deleteStatus ) {
        res.statusCode = 200;
        res.json("Order status deleted sucessfully");
    }
})

router_order_status.patch('/status/:id', validateStatusProps, validateToken, validateUserRol, async ( req, res ) => {
    const statusId = req.params.id;

    let updateStatusId = await OrderStatusController.updateOrderStatus( statusId, req.body.description );
    //Analyze if update was made sucessfully
    if( updateStatusId.ok === 1 ){
        res.statusCode = 200;
        res.json("Order status updated sucessfully");
    }
})


/*---- Middlewares -----*/

//function that validates properties sent by request
function validateStatusProps(  req, res, next ) {
    const { description } = req.body;

    if( !description ) {
        res.statusCode = 400;
        res.json("Invalid properties");
    } else {
        next();
    }
}


//function that verifies token generated
function validateToken( req, res , next ) {
    try { 
        const token = req.headers.authorization.split(' ')[1];
        const verifyToken = jwt.verify( token, jwtSign );

        if( verifyToken ) {
            return next();
        } 
    } catch( error ) {
        res.statusCode = 401;
        res.json(error);
  }
}

//function that validates if user has admin role
async function validateUserRol( req, res , next ) {
    try { 
        const token = req.headers.authorization.split(' ')[1];
        const verifyToken = jwt.verify( token, jwtSign );
        //find user id by username in Users table
        let rolDescription = await userController.getUserId( verifyToken )
        //find role id by user id in UserRole table
        .then( async (userId) => await userController.getRoleIdBy( userId )
            //find role description by role id 
            .then( async (roleId) => await rolController.getRoleby( roleId )
                .then( async (rolDesc) => rolDesc )
            )
        );
        if( rolDescription === ROLE_ADMIN_DESCRIPTION ) {
            next();
        } else {
            res.statusCode = 401;
            res.json("User not authorized");
        }
 
    } catch( error ) {
        res.statusCode = 401;
        res.json(error); 
  }
}


module.exports = router_order_status;