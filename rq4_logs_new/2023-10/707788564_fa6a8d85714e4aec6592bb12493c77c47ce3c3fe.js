var express = require('express');
var router = express.Router();
// Define the "computation" route
router.get('/', function(req, res, next) {
 var x = parseFloat(req.query.x) || (Math.random() * 10);
 var angleRadians = calcAngle(x, 10); // Assuming a hypotenuse of 10
 // Format the response string
 var responseString = `Math.cosh applied to ${x} is ${angleRadians}`;
 // Send the response
 res.send(responseString);
});

function calcAngle(adjacent, hypotenuse) {
 return Math.cosh(adjacent / hypotenuse);
}
module.exports = router;