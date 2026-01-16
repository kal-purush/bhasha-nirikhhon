
var mongoose = require('mongoose');
var Garage = mongoose.model('Garage');

//GET - Return all registers
exports.findAll = function(req, res) {
    console.log('\n--------------------------------------------------------');
    console.log('GET /garages')
    console.log('--------------------------------------------------------\n');

    Garage.find(function(err, garages) {
    
    if(err){
        res.send(500, err.message);
    }
    else{
        console.log("Garages sent:")
        console.log("-------------\n");
        console.log(garages);
        console.log("\n");
        res.status(200).jsonp(garages);
    }
    });
};

//GET - Return a register with specified ID
exports.findById = function(req, res) {
 Garage.findById(req.params.id, function(err, garage) {
 if(err) return res.send(500, err.message);
 console.log('GET /garages/' + req.params.id);
 res.json(garage);
 //res.status(200).jsonp(garage);
 });
};

//POST - Insert a new register
exports.add = function(req, res) {
 console.log('POST');
 console.log(req.body);
 var garage = new Garage({
 garage: req.body.garage,
 estado: req.body.estado
 });
 garage.save(function(err, garage) {
 if(err) return res.send(500, err.message);
 res.status(200).jsonp(garage);
 });
};

//PUT - Update a register already exists
exports.update = function(req, res) {
 Garage.findById(req.params.id, function(err, garage) {
 garage.garage = req.body.garage;
 garage.estado = req.body.estado
 garage.save(function(err) {
 if(err) return res.send(500, err.message);
 res.status(200).jsonp(garage);
 });
 });
};

//DELETE - Delete a register with specified ID
exports.delete = function(req, res) {
 Garage.findById(req.params.id, function(err, garage) {
 garage.remove(function(err) {
 if(err) return res.send(500, err.message);
 res.json({ message: 'Successfully deleted' });
 });
 });
};