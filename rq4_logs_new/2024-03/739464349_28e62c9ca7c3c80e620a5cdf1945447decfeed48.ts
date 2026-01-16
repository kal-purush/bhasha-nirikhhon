import { REACTION_ROUTE } from './constants';

import type { Router, Request, Response } from 'express';

import { Reaction } from '../db-models/reaction';

export const reactionRoutes = (router: Router) => {
	router.post(REACTION_ROUTE, async (req: Request, res: Response) => {
		try {
			const body = req.body;
			const { reaction, commentId } = body ?? {};

			if (!reaction) {
				res.status(400);
				res.send('Bad request');
				return;
			}

			await Reaction.create({
				comment_id: commentId,
				reaction
			});

			res.status(200);
			res.json('OK');
		} catch (err) {
			console.error(err);
		}
	});
	return router;
};