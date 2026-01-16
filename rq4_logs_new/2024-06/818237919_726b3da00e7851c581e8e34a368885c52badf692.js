// Create web server

// Import modules
const express = require('express');
const bodyParser = require('body-parser');
const router = express.Router();
const commentController = require('../controllers/commentController');

//Handle request
router.post('/add', commentController.addComment);
router.get('/list', commentController.listComment);
router.get('/list/:id', commentController.listCommentById);
router.put('/update/:id', commentController.updateComment);
router.delete('/delete/:id', commentController.deleteComment);

module.exports = router;


