import dotenv from 'dotenv';
import { Request, Response } from 'express';
import bcrypt from 'bcryptjs';
import { getToken } from '../utilities/utilities';
import prisma from '../prisma';

dotenv.config();

export async function userSignup(req: Request, res: Response) {
    let { name, email, password } = req.body;

    if (!name) return res.status(400).json({ success: false, msg: 'Name cannot be empty!' });
    if (!email) return res.status(400).json({ success: false, msg: 'Email cannot be empty!' });
    if (!password) return res.status(400).json({ success: false, msg: 'Password cannot be empty!' });

    // Check if email already exists
    const oldUser = await prisma.user.findFirst({
        where: {
            email: email,
        }
    });

    if (oldUser) {
        return res.status(409).send("User Already Exist!");
    }

    const encryptedPassword = await bcrypt.hash(password, 10);

    try {
        const user = await prisma.user.create({
            data: {
                name: name,
                email: email,
                password: encryptedPassword
            },
            select: {
                id: true,
                name: true,
                email: true,
            }
        })

        const token = getToken(user.id);
        res.cookie('jwt_token', token, { httpOnly: true, maxAge: 1000 * 3600 * 24 });
        res.status(201).json({ data: user, token: token })
    } catch (error) {
        res.status(500).json('Something went wrong!')
    }
}

export async function userLogin(req: Request, res: Response) {
    const { email, password } = req.body;

    if (!email) return res.status(400).json({ success: false, msg: 'Email cannot be empty!' });
    if (!password) return res.status(400).json({ success: false, msg: 'Password cannot be empty!' });

    const user = await prisma.user.findFirst({
        where: {
            email: email,
        }
    });

    if (!user) return res.status(400).json("Invalid Credentials!");

    if (user && (await bcrypt.compare(password, user.password))) {
        // Create token
        const token = getToken(user.id);
        res.cookie('jwt_token', token, { httpOnly: true, maxAge: 1000 * 3600 * 24 });
        return res.status(200).json({ token: token, user_id: user.id });
    }

    res.status(400).json("Invalid Credentials!");
}