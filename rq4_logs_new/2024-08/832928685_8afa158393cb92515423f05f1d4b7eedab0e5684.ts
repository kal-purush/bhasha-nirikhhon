import { Request, Response, NextFunction } from 'express';

export function authorizer(req: Request, res: Response, next: NextFunction) {
	const token = req.headers.authorization;
	console.log(token, 'token');
	if (!token) {
		res.status(401).json({ message: 'Unauthorized' });
		return;
	}
	const customer = getUserByToken(token);
	req.body.customer = customer;
	if (customer) {
		next();
	} else {
		res.status(401).json({ message: 'Unauthorized' });
	}
}

function getUserByToken(token: string): any {
	// This is a dummy implementation. Replace this with actual implementation
	return { id: '1', name: 'Anuja Verma' };
}