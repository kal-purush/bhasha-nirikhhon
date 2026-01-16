const mongoose = require("mongoose");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");

require("dotenv").config();
const _ = require('lodash');
const bookingUtils = require("../utils/bookingUtils")
const db = require("../models/index");


exports.add_flight = async (req, res) => {
  try {
    const {
      airline,
      flightNumber,
      departure,
      arrival,
      departureAirport,
      arrivalAirport,
      price,
    } = req.body;

    var flightData = {
      airline: airline,
      flightNumber: flightNumber,
      departure: departure,
      arrival: arrival,
      departureAirport: departureAirport,
      arrivalAirport: arrivalAirport,
      flightDuration: Math.trunc(Math.abs((new Date(arrival) - new Date(departure)) / 60000)),
      price: price,
    }

    flightData = await db.flight.create(flightData);

    return res.status(200).json({ message: "flight added", flightData });
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: error.message });
  }
};

exports.remove_flight = async (req, res) => {
  try {
    const { flightId } = req.params

    data = await db.flight.findByIdAndDelete(flightId)

    return res.status(200).json({ message: "flight removed" });

  } catch (error) {
    console.log(error);
    res.status(500).json({ error: error.message });

  }
}

exports.flightsDataForAdmin = async (req, res) => {
  try {
    const { airline, date, departureAirport, arrivalAirport } = req.query // date format 02-12-22

    searchDate = new Date(date)
    nextDate = new Date((new Date(searchDate).getTime()) + 60 * 60 * 24 * 1000);

    query = {
      airline: airline,
      departureAirport: departureAirport,
      arrivalAirport: arrivalAirport,
    }
    if (date) {
      query.departure = {
        $gte: searchDate,
        $lt: nextDate
      }
    }
    query = _.pickBy(query, _.identity)

    flightData = await db.flight.find(query).select('airline flightNumber arrival departure departureAirport arrivalAirport flightDuration price availableSeats')

    return res.status(200).json(flightData);

  } catch (error) {
    console.log(error);
    return res.status(500).json({ error: error.message });
  }
}

exports.flightAllData = async (req, res) => {
  try {
    const { flightId } = req.params

    flightData = await db.flight.findByIdAndDelete(flightId).select('airline flightNumber arrival departure departureAirport arrivalAirport flightDuration price availableSeats')

    return res.status(200).json(flightData);

  } catch (error) {
    console.log(error);
    res.status(500).json({ error: error.message });
  }
}

exports.bookings = async (req, res) => {
  try {
    const { flightId } = req.query

    query = {
      flight: flightId,
    }
    query = _.pickBy(query, _.identity)
    update = await bookingUtils.updateStatus()

    flightData = await db.booking.find(query)
      .populate('user', 'fullName email')
      .populate('flight', 'airline flightNumber departure arrival departureAirport arrivalAirport flightDuration')

    return res.status(200).json(flightData);

  } catch (error) {
    console.log(error);
    res.status(500).json({ error: error.message });

  }
}