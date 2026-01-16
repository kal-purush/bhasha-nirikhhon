const express = require ('express');
const router = express.Router ();


const controller = require ('../controllers/reviewsController'); 
router.post('/crearReview', controller.create);


module.exports = router