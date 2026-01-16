import { Request, Response } from 'express';
import * as userService from '../services/User';
import jwt from "jsonwebtoken";
import { User } from '../models/User';
import { getRepository } from 'typeorm';
import bcrypt from 'bcrypt'



export const createUser = async (req: Request, res: Response) => {
    try {

        
        const newUser = await userService.createUserService(req.body);

        return res.status(200).json(newUser);
        
    } catch (error) {
        return res.status(500).json({ error: error.message || 'Internal Server Error' });
    }
};

export const createPassword = async (req: Request, res: Response) => {
    const { token } = req.params;
    const { password } = req.body;
  
    try {

      const decoded: any = await jwt.verify(token, process.env.JWT_SECRET || "key");
      const userId = decoded.userId;
  

      const userRepository = getRepository(User);
      const userToUpdate = await userRepository.findOne({  where: { id: userId }, });
  
      if (!userToUpdate) {
        return res.status(404).json({ message: "User not found" });
      }
      
      const hashedPassowrd = await bcrypt.hash(password,10);

      userToUpdate.password = hashedPassowrd;
  

      await userRepository.save(userToUpdate);
  
      return res.status(200).json({ message: "Password updated successfully" });
    } catch (err) {
      console.log(err);
      return res.status(500).json({ message: "Internal Server Error" });
    }
  };

