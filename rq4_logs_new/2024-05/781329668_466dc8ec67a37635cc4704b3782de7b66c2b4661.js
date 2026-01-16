import { Router } from 'express';
import paginate from '../pagination.js';
import db from '../models/index.js';

const router = Router();

router.get('/', async (req, res) => {
    let [posts, pagination] = await paginate(db.Post, req.query.page, 20);
    res.render('posts/index.njk', {posts, pagination});
});

router.get('/create', async (req, res) => {
    res.render('posts/create.njk');
});

router.post('/', async (req, res) => {
    await db.Post.create({
        title: req.body.title,
        body: req.body.body
    });
    res.redirect('/posts');
});

router.get('/edit/:id', async (req, res) => {
    let post = await db.Post.findByPk(req.params.id);
    res.render('posts/edit.njk', {post});
});

router.post('/edit/:id', async (req, res) => {
    await db.Post.update({
        title: req.body.title,
        body: req.body.body
    }, {
        where: {
            id: req.params.id
        }
    });
    res.redirect('/posts');
});

router.get('/delete/:id', async (req, res) => {
    await db.Post.destroy({
        where: {
          id: req.params.id,
        },
    });
    res.redirect('/posts');
});

router.get('/specific/:id', async (req, res) => {
    let post = await db.Post.findByPk(req.params.id);
    res.render('posts/specific.njk', {post});
});

export default router;