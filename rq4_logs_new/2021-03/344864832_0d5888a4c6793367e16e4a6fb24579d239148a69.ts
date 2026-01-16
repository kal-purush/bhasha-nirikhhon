import express from 'express';
import { likeService } from '../services/likeService';
import { LikeModel } from '../models/Like';

const likeValue = (v: number) => v > 0 ? 1 : v < 0 ? -1 : 0;
class LikeController {
    async setLike(req: express.Request, res: express.Response) {
        const { userId, postId, value } = req.body;
        const like = likeService.findPostLikes({ userId, postId }) as unknown as LikeModel;
        if (!like) {
            try {
                likeService.saveLike({
                    value: likeValue(value),
                    userId,
                    postId
                } as LikeModel)
                res.status(200).send();
            } catch (e) {
                return res.status(500).send({
                    type: 'ERROR',
                    message: 'Error occurs setting like'
                });
            }
        }
        res.status(400).send({
            type: 'ERROR',
            message: 'Cannot create same like twice'
        });
    }

    async changeLike(req: express.Request, res: express.Response) {
        const { userId, postId, value } = req.body;
        const likeId = req.params.id as string;
        const like = likeService.findPostLikes({ _id: likeId }) as unknown as LikeModel;
        if (like && like.userId === userId && like.postId === postId) {
            like.value = likeValue(value);
            like.save();
            res.status(200).send();
        } else {
            res.status(404).send({
                type: 'ERROR',
                message: 'Cannot find like'
            });
        }
    }
}

const likeController = new LikeController();

export default likeController;