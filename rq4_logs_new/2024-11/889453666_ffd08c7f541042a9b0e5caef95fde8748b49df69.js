import UserModel from "../models/User.js";
import ReservationModel from "../models/Reservation.js";

const reserveAGuide = async (req, res, next) => {
  try {
    const user = await UserModel.findById(req.user._id);
    if (!user) {
      return res.status(200).json({ message: "User not found!" });
    }

    const {location,timePeriod,people,cost} = req.body;

    const newReservation = new ReservationModel({
      location,
      timePeriod,
      people,
      cost,
      user:user,
      guide
    })
    
    
  
  } catch (error) {
    next(error);
  }
};