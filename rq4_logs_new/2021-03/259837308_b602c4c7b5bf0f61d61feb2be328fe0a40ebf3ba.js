var braintree = require("braintree");

var gateway = braintree.connect({
  environment: braintree.Environment.Sandbox,
  merchantId: process.env.MERCHANT_ID,
  publicKey: process.env.PUBLIC_KEY,
  privateKey: process.env.PRIVATE_KEY
})




exports.getToken = (req, res) => {
    gateway.clientToken.generate({
        //customerId: aCustomerId
      }, function (err, response) {
            if(err) {
                res.status(500).send(err);
            }
            res.send(response);
      });
}



exports.processPayment = (req, res) => {

    let amountFromTheClient = req.body.amout;
    let nonceFromTheClient = req.body.nonceFromTheClient

    gateway.transaction.sale({
        amount: amountFromTheClient,
        paymentMethodNonce: nonceFromTheClient,
        //deviceData: deviceDataFromTheClient,
        options: {
          submitForSettlement: true
        }
      }, function (err, result) {
       
          if(err) {
              res.status(500).send(err)
          }
          res.send(result);
      });
}