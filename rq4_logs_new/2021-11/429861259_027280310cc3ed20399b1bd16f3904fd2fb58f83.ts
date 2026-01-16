import { Request } from 'express';
import * as UserService from './user.service';
import { ApiResponse } from '../../types';
import { createTokens } from '../../utils/auth';

export async function signUpHandler(req: Request, res: ApiResponse) {
	const newUser = await UserService.signUp(req.body);
	const userToken = await createTokens(newUser);

	res.set('Access-Control-Expose-Headers', 'x-access-token, x-refresh-token');
	res.set('x-access-token', userToken.accessToken);
	res.set('x-refresh-token', userToken.refreshToken);

	res.status(201).json({ status: 201, message: 'user registered successfully' });
}