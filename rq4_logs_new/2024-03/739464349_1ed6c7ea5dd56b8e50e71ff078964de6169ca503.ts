import CommentService from './service';

import type { Request, Response } from 'express';

import { REQUEST_ERROR, SERVER_ERROR } from '../constants/messages';
import * as console from 'console';

class CommentController {
	public getComments = async (request: Request, response: Response) => {
		try {
			const { topicId, offset, limit = 0 } = request.params;

			if (!topicId || !offset || limit < 0) {
				response.status(400).json(REQUEST_ERROR);
			}

			const topics = await CommentService.getComments({
				data: {
					topicId: Number(topicId),
					offset: Number(offset),
					limit: Number(limit)
				}
			});

			if (topics) {
				response.json(topics).status(200);
			}
		} catch (e) {
			response.status(500).json({
				response: SERVER_ERROR,
				reason: e
			});
		}
	};
	public createComment = async (request: Request, response: Response) => {
		try {
			const { body } = request;

			const comment = await CommentService.createComment(body);

			if (comment) {
				response.json(comment).status(200);
			}
		} catch (e) {
			if (e instanceof Error) {
				console.error(e, e.stack);
			}
			response.status(500).json({
				response: SERVER_ERROR,
				reason: e
			});
		}
	};

	public updateComment = async (request: Request, response: Response) => {
		try {
			const { body } = request;
			const { commentId } = request.params;

			if (!commentId) {
				response.status(400).json(REQUEST_ERROR);
			}

			await CommentService.updateComment({
				data: body,
				commentId: Number(commentId)
			});

			response.json('OK').status(200);
		} catch (e) {
			response.status(500).json({
				response: SERVER_ERROR,
				reason: e
			});
		}
	};

	public deleteComment = async (request: Request, response: Response) => {
		try {
			const { commentId } = request.params;

			if (!commentId) {
				response.status(400).json(REQUEST_ERROR);
			}

			await CommentService.deleteComment(Number(commentId));

			response.status(204);
		} catch (e) {
			response.status(500).json({
				response: SERVER_ERROR,
				reason: e
			});
		}
	};
}

export default new CommentController();