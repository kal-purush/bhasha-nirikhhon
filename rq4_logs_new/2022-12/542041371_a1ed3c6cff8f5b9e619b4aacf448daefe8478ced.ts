import { check } from 'express-validator';
import { CommentController } from '../controllers/index';
import authMiddleware from '../middleware/authMiddleware';

const Router = require('express');

const router = new Router();

router.post(
  '/',
  authMiddleware,
  [
    check('content', 'Контент не может быть пустым').notEmpty(),
    check('postId', 'Укажите id поста').notEmpty().isNumeric(),
  ],
  CommentController.create
);
router.get('/', authMiddleware, CommentController.getAll);

export default router;