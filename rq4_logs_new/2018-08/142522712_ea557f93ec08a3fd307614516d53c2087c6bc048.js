//middleware to add and validate address model
const express = require('express')
const mongoose = require('mongoose')
const AddressModel = mongoose.model('Address')
const shortid = require('shortid')
const response = require('../libs/responseLib')
const time = require('../libs/timeLib')
const check = require('../libs/checkLib')
const logger = require('../libs/loggerLib')

let addToAddressDB = (req, res, next) => {
	req.addressId = shortid.generate()
	let newAddress = new AddressModel({
		addressId: req.addressId,
		houseNo: req.body.houseNo,
		street: req.body.street,
		area: req.body.area,
		city: req.body.city,
		pincode: req.body.pincode
	})
	newAddress.save((err, result) => {
		if (err) {
			logger.error(err.message, 'Address Middleware: addToAddressDB', 10)
			let apiResponse = response.generate(true, 'Error occured while adding address', 500, null)
			res.send(apiResponse)
		} else {
			logger.info('Address successfully Created', 'Address Middleware: addToAddressDB', 5)
			next()
		}
	})
}

let removeFromAddressDB = (req, res, next) => {
	AddressModel.findOneAndRemove({'addressId' : req.params.addressId },(err, result) => {
		if (err) {
			logger.error(err.message, 'Address Middleware: removeFromAddressDB', 10)
			let apiResponse = response.generate(true, 'Error occured while deleting the address', 500, null)
			res.send(apiResponse)
		} else if (check.isEmpty(result)) {
			logger.info('No Address Found', 'Address Middleware: removeFromAddressDB', 5)
			let apiResponse = response.generate(true, 'No Address Found', 404, null)
			res.send(apiResponse)
		} else {
			logger.info('Address successfully Deleted', 'Address Middleware: removeFromAddressDB', 5)
			next()
		}
	})
}

let updateAddressDB = (req, res, next) => {
	AddressModel.findOneAndUpdate({'addressId' : req.params.addressId },{'houseNo':req.body.houseNo,'street':req.body.street,'area':req.body.area,'city':req.body.city,'pincode':req.body.pincode},{new : true},(err, result) => {
		if (err) {
			logger.error(err.message, 'Address Middleware: updateAddressDB', 10)
			let apiResponse = response.generate(true, 'Error occured while updating the address', 500, null)
			res.send(apiResponse)
		} else if (check.isEmpty(result)) {
			logger.info('No Address Found', 'Address Middleware: updateAddressDB', 5)
			let apiResponse = response.generate(true, 'No Address Found', 404, null)
			res.send(apiResponse)
		} else {
			logger.info('Address successfully Updated', 'Address Middleware: updateAddressDB', 5)
			next()
		}
	})
}

module.exports = {
    addToAddressDB: addToAddressDB,
	removeFromAddressDB : removeFromAddressDB,
	updateAddressDB : updateAddressDB
}