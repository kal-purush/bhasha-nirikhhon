import { HttpError, NotFoundError } from 'routing-controllers';
import { Op } from 'sequelize';

import { Comment } from '../models/comment';
import { User } from '../models/user';

class CommentService {
  async create(body: { content: string; topicId: number; parentId?: number }, userId: number) {
    const { parentId } = body;

    if (parentId) {
      const parentComment = await Comment.findByPk(parentId);

      if (!parentComment) {
        throw new NotFoundError('Нет родительского комментария с таким id');
      }
    }

    const createdComment = await Comment.create({ ...body, userId });

    return createdComment.get({ plain: true });
  }

  async delete(id: number, userId: number) {
    const comment = await Comment.findByPk(id);

    if (!comment) {
      throw new NotFoundError('Комментарий не найден');
    }

    if (comment.dataValues.userId !== userId) {
      throw new HttpError(403, 'У вас нет прав удалить этот комментарий');
    }

    await comment.destroy();

    return true;
  }

  async findAllByTopicId(topicId: number) {
    const comments = await Comment.findAll({
      order: [['createdAt', 'DESC']],
      where: {
        topicId: {
          [Op.eq]: topicId,
        },
      },
      include: [
        {
          model: User,
          attributes: ['id', 'login', 'avatar'],
        },
      ],
    });

    return comments.map((item) => item.get({ plain: true }));
  }
}

export default new CommentService();