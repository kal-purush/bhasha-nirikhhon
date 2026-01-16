import { Request, Response } from 'express';
import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';

import { User } from '../models/user';

const { JWT_SECRET } = process.env;

export const login = async (req: Request, res: Response): Promise<void> => {
    try {
        const { email, password } = req.body;

        const user = await User.findOne({ email });

        if (!user) {
            res.status(401).json({ message: "Email/Password mismatch" });
            return;
        }

        const isMatch = await bcrypt.compare(password, user.password);
        if (!isMatch) {
            res.status(401).json({ message: "Email/Password mismatch" });
            return;
        }

        const token = jwt.sign({ userId: user._id }, JWT_SECRET as string, { expiresIn: '1h' });

        res.status(200).json({ token, userId: user._id });
    } catch (error) {
        res.status(500).json({ message: "Something went wrong...", error });
    }
};