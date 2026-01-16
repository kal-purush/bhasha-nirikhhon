import { Request, Response } from 'express';
const { userModel } = require('../models/mongoSchemas');

// Get user by userName and userPassword
const getUser = async (req: Request, res: Response): Promise<Response> => {
    const { userName, userPassword } = req.params;
    const user = await userModel.findOne({ userName, userPassword });
    return res.status(200).json(user);
};

// Create new user

// Create holiday booking for user (user can have multiple bookings) - this is a POST request

// Get all bookings for user - this is a GET request

//export
module.exports = {
    getUser,
}