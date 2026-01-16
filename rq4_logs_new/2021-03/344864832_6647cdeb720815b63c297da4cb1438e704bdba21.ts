import express from 'express';
import { commentService } from '../services/commentService';
import { CommentModel } from '../models/Comment';

class CommentController {
    async createComment(req: express.Request, res: express.Response) {
        const text = req.body.text as string;
        const userId = req.headers.id as string;
        if (!text || !text.trim()) {
            return res.status(400).send({
                type: 'ERROR',
                message: 'Invalid comment data'
            });
        }

        try {
            const comment = await commentService.createComment({text, userId} as CommentModel);
            return res.status(200).send({
                id: comment._id,
                text,
                userId
            })
        } catch (e) {
            return res.status(500).send({
                type: 'ERROR',
                message: 'Error occurs during post creation'
            });
        }
    }

    async updateComment(req: express.Request, res: express.Response) {
        const text = req.body.text as string;
        const userId = req.headers.id as string;
        const commentId = req.params.id as string;

        if (!text || !text.trim()) {
            return res.status(400).send({
                type: 'ERROR',
                message: 'Invalid comment data'
            });
        }

        try {
            await commentService.updateComment(commentId, userId, {text} as CommentModel);
            return res.status(200).send({
                id: commentId,
                text
            })
        } catch (e) {
            return res.status(500).send({
                type: 'ERROR',
                message: 'Error occurs during post creation'
            });
        }

    }

    async readComment(req: express.Request, res: express.Response){
        const userId = req.headers.id as string;
        const commentId = req.params.id as string;
        const comment = await commentService.findCommentBy({_id: commentId, userId});

        if (!comment) {
            return res.status(404).send({
                type: 'ERROR',
                message: 'Comment not found'
            });
        } else {
            return res.status(200).send({
                comment
            }) 
        }
    }

    async deleteComment(req: express.Request, res: express.Response) {
        const commentId = req.params.id as string;
        const userId = req.headers.id as string;

        return commentService.deleteComment({_id: commentId, userId});
    }
}

const commentController = new CommentController();

export default commentController;