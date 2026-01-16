import { Request, Response } from 'express';
import { User } from 'models';
import { BadRequestError, DatabaseConnectionError } from 'libs/errors';
import { Password } from 'libs/passwords';
import jwt from 'jsonwebtoken';
import { env } from 'config/environment';
import { response } from 'utils';

export const currentUser = async (req: Request, res: Response) => {
    const { user } = req;
    response(res, 200, true, { currentUser: user || null });
};

export const signIn = async (req: Request, res: Response) => {
    const { email, password } = req.body;

    const existingUser = await User.findOne({ where: { email } });
    if (!existingUser) throw new BadRequestError("User doesn't exist");

    const passwordsMatch = await Password.compare(existingUser.password, password);
    if (!passwordsMatch) throw new BadRequestError('Invalid password');

    const token = jwt.sign({ id: existingUser.id, email: existingUser.email }, env.JWT_SECRET, {});

    req.session = { jwt: token };

    response(res, 200, true, { currentUser: existingUser });
};

export const signUp = async (req: Request, res: Response) => {
    const { email, password } = req.body;

    const existingUser = await User.findOne({ where: { email } });

    if (existingUser) throw new DatabaseConnectionError('User exists!', 400);

    const user = User.build({ email, password });
    await user.save();

    const token = jwt.sign({ id: user.id, email: user.email }, env.JWT_SECRET, {});

    req.session = { jwt: token };

    response(res, 201, true, { currentUser: user });
};

export const signOut = async (req: Request, res: Response) => {
    req.session = null;
    response(res, 200, true, { currentUser: null });
};