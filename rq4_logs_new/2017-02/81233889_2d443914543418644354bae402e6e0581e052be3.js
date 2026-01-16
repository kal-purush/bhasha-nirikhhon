const express = require('express');
const router = express.Router();
const bodyParser = require('body-parser');
const jsonParser = bodyParser.json();

const {BlogPosts} = require('./models');

BlogPosts.create(
    "Express Router",
    "Express router helps us modularize our application endpoints.",
    "Allen Gordon"
)
BlogPosts.create(
    "RESTful APIs",
    "REpresentation State Transfer is a style of software architecture, a set of principles for designing APIs.",
    "Allen Gordon"
)

router.get('/', (req, res) => {
    res.json(BlogPosts.get());
});

router.post('/', jsonParser, (req, res) => {
    const requiredFields = ['title', 'content', 'author'];
    for(let i = 0; i < requiredFields.length; i++) {
        const field = requiredFields[i];
        if(!(field in req.body)) {
            const message = `Missing \`${field}\` in request body`
          console.error(message);
          return res.status(400).send(message);
        }
    }
    const post = BlogPosts.create(
        req.body.title,
        req.body.content,
        req.body.author
    )
    res.status(201).json(post);
})

router.put('/:id', jsonParser, (req, res) => {
    const requiredFields = ['title', 'content', 'author'];
    for(let i = 0; i < requiredFields.length; i++) {
        const field = requiredFields[i];
        if(!(field in req.body)) {
            const message = `Missing \`${field}\` in request params`
            console.error(message);
            return res.status(400).send(message);
        }
    }
    if(req.params.id !== req.body.id) {
        const message = (
            `Request path id (${req.params.id}) and request body id `
            `(${req.body.id}) must match`);
        console.error(message);
        return res.status(400).send(message);
    }
    const updatedPost = BlogPosts.update({
        id: req.params.id,
        title: req.body.title,
        content: req.body.content,
        author: req.body.author
    })
    res.status(204).json(updatedPost);
})

router.delete('/:id', (req, res) => {

    BlogPosts.delete(req.params.id);
    res.status(204).end();
})

module.exports = router