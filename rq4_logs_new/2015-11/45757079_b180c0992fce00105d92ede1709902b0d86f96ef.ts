/// <reference path="../../typings/tsd.d.ts" />
import * as braintree from "braintree";
import * as express from "express";

export default function bt (router: express.Router) {
	
	var gateway = braintree.connect({
		environment: braintree.Environment.Sandbox,
		merchantId: "sgxtk48ffb2q6jnw",
		publicKey: "dtk9d584sgtsxzvd",
		privateKey: "6d330cab6b77ac7711e4252a5b6c483e"
	});
	
	router.get("/client_token", function (req, res) {
		gateway.clientToken.generate({}, function (err, response) {
			if (err) res.sendStatus(503);
			else res.send(response.clientToken);
		});
	});
	
	router.post("/checkout", function (req, res) {
		var nonce = req.body.payment_method_nonce;
		
		gateway.transaction.sale({
			amount: '10.00',
			paymentMethodNonce: nonce,
		}, function (err, result) {
			
		});
	});
	
	return router;
}