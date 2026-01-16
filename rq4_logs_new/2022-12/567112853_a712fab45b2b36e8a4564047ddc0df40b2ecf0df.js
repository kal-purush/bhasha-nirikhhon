const db = require("../../db/conn");
var Publishable_Key = 'pk_test_51M91kVSHQvfYHLAWyAahZV2KZT8GsSA7CRDM7cB1UazY0LC7autWLpqc7JBitHbqTUykJyr8Rm5Zt27Gj8VOGI2h00AgaKrSzJ'
var Secret_Key = 'sk_test_51M91kVSHQvfYHLAWtC9g5Qj15lBIcaY8TTA1xpd30Lg853d05zVOooEDb84dzodKtJMy2cSE5tzTjc5vPi9cmHz300VSzWjPGE'
const stripe = require('stripe')(Secret_Key)
const card_token = {
    card_name:"Atullsenn",
    card_number:"4242424242424242",
    exp_Month:12,
    exp_year:2023,
    card_cvc:"314"

 }

 

const paymentGateway = (req,res)=>{
    stripe.customers.create({
        email: req.body.email,
        source:"card_1M9nZMSHQvfYHLAWzmY55wpt",
        address:{
            line1:req.body.line1,
            postal_code:req.body.postal_code,
            city:req.body.city,
            state:req.body.state,
            country:req.body.country
        }
    })
    .then((customer) => {
 
        return stripe.charges.create({
            amount: 5000,    
            description: 'Web Development Product',
            currency: 'usd',
            customer: customer.id
        });
    })
    .then((charge) => {
        res.status(200).send({
            success: true,
            message: "Payment Success",
            data:charge
        })  
    })
    .catch((err) => {
        res.status(500).send({
            success: false,
            message:err
        })      
    });


}



module.exports = paymentGateway;


module.exports.addNewCard = async(req, res, next) =>{
    const {
    customer_Id,
    card_Name,
    card_ExpYear,
    card_ExpMonth,
    card_Number,
    card_CVC,
    } = req.body;
    
    try {
    const card_Token = await stripe.tokens.create({
    card: {
    name: card_Name,
    number: card_Number,
    exp_month: card_ExpMonth,
    exp_year: card_ExpYear,
    cvc: card_CVC,
    },
    });
    
    const card = await stripe.customers.createSource(customer_Id, {
    source: `${card_Token.id}`,
    });
    
    return res.status(200).send({ card: card.id });
    } catch (error) {
    throw new Error(error);
    }
    }

    //checking payment api
 module.exports.createNewCustomer = async(req, res, next) =>{
    try{
    const customer = await stripe.customers.create({
    name: req.body.name,
    email: req.body.email,
    });
    res.status(200).send(customer);
    }catch(error){
    throw new Error(error);
    }
    }


    module.exports.createCharges = async(req, res, next) =>{
        try{
        const createCharge = await stripe.paymentIntents.create({
            amount: req.body.amount,
            currency: 'usd',
            description: 'Mobapps Solutions Private Limited',
            payment_method_types: ['card'],
          }); 
        res.send(createCharge);
        }catch(err){
        //throw new Error(err);
        console.log(err)
        res.status(500).send({
            success: false,
            message:'err',
            data:err
        })
        }
        }






 //chekcing  payment api