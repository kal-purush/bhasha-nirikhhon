import { Response, Request } from "express";
import User from "../models/User";
import bcrypt from "bcryptjs";

export type Credentials = {
    email: string,
    password: string
}

export const userLogin = async (req: Request, res: Response): Promise<any> => {
    const result = await User.findOne({ email: req.body.email }).exec();
    if(!result){
        return res.status(400).json({ emailnotfound: "Email not found" });
    }

    const isValidPassword = await bcrypt.compare(req.body.password, Object.values(result)[Object.values(result).length -2].password);

    if(isValidPassword){
        res.status(200).json({message: "Logged in"});
    }else{
        res.status(400).json({message: "Nu i bine"});
    }

};

export const userRegister = async (req: Request, res: Response): Promise<Response> => {

    const result = await User.findOne({ email: req.body.email });
    if(result){
        return res.status(400).json({email: "Email already exists"});
    }else{

        const hashedPassword = await bcrypt.hash(req.body.password, 12);

        const newUser = new User({
            email: req.body.email,
            password: hashedPassword,
            isAdmin: false
        })

        const response = await newUser.save();
        return res.status(200).json({message: "Merge"});
    }
};

export const adminRegister = async (req: Request, res: Response): Promise<Response> => {

        const hashedPassword = await bcrypt.hash(req.body.password, 12);

        const newUser = new User({
            email: req.body.email,
            password: hashedPassword,
            isAdmin: true
        })

        await newUser.save();

        return res.status(200).json({message: "New admin added successfully"});
};