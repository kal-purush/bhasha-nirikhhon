import express = require('express');
import { validateParams } from '../helpers/validation';
import { plainToClass } from 'class-transformer';
import { getRepository } from 'typeorm';
import { User } from '../entities/user';
import { Post } from '../entities/post';
import { Reply } from '../entities/reply';
import { Like } from '../entities/like';
import Boom = require('@hapi/boom');

const router = express.Router();



// Like comment
// Fix routing, use body instead of params.
// Check that user hasn't already liked
router.post('/:user/:post/:comment',  async (req, res, next) => {
    try {
        const user = plainToClass(User, await getRepository(User).findOne(req.params.user));
        const post = plainToClass(Post, await getRepository(Post).findOne(req.params.post));
        const comment = plainToClass(Reply, await getRepository(Reply).findOne(req.params.reply));
        if (user && post && comment) {
            const like: Like = new Like(user, post, comment);
            await getRepository(Like).save(like);
            res.status(200).send(like);
        }
        else {
            next(Boom.notFound())
        }
    } catch (error) {
        next(Boom.badImplementation());
    }
})

// Like post
// Fix routing, use body instead of params.
// Check that user hasn't already liked
router.post('/:user/:post/',  async (req, res, next) => {
    try {
        const user = plainToClass(User, await getRepository(User).findOne(req.params.user));
        const post = plainToClass(Post, await getRepository(Post).findOne(req.params.post));
        if (user && post) {
            const like: Like = new Like(user, post);
            await getRepository(Like).save(like);
            res.status(200).send(like);
        }
        else {
            next(Boom.notFound())
        }
    } catch (error) {
        next(Boom.badImplementation());
    }
})


// Unlike post
// Fix routing, use body instead of params.
router.delete('/:user/:post/', async (req, res, next) => {
    try {
        const deleted = await getRepository(Like).createQueryBuilder().delete().where({userId : req.params.user, postId : req.params.post, replyId : null}).execute();
        if(deleted.affected) {
            res.sendStatus(204);
        }
        else {
            next(Boom.notFound());
        }
    } catch (error) {
        console.log(error);
        next(Boom.badImplementation());
    }
        

    })


// Unlike comment
// Fix routing, use body instead of params.
router.delete('/:user/:post/:comment', async (req, res, next) => {
    try {
        const deleted = await getRepository(Like).createQueryBuilder().delete().where("userId = :user, postId = :post, replyId = :comment", {user : req.params.user, post : req.params.post, comment : req.params.comment}).execute();
        if(deleted.affected) {
            res.sendStatus(204);
        }
        else {
            next(Boom.notFound());
        }
    } catch (error) {
        next(Boom.badImplementation());
    }
        

    })




module.exports = router;