import { Request, Response, NextFunction } from "express";
import RegistrationToken from "../models/RegistrationToken";
import User from "../models/User";

// Route for verifying a user

const RegistrationTokenController = {
    async verify(req: Request, res: Response) {
        try{
            const token = req.params.token;
            console.log(token);
            const tokenInfo = await RegistrationToken.findOne({ token: token });
            if (!tokenInfo) {
                return res.status(400).json({ message: "Token is invalid" });
            }
            if (tokenInfo.expiry && tokenInfo.expiry < new Date()) {
                return res.status(400).json({ message: "Token is expired" });
            }
            const user = await User.findById(tokenInfo.userId);
            console.log(user);
            // return username, email, password in response json
            res.status(200).json({ message: "User verified", user });
        }
        catch(error){
            res.status(500).json({ message: "Error verifying user", error });
        }
        // const token = req.params.token;
        // console.log(token);
        // return res.status(200);
    }
};

export default RegistrationTokenController;