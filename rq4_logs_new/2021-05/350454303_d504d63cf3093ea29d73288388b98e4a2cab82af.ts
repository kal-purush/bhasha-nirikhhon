import { Request, Response } from 'express';

import { ForumComment, ForumTopic } from '../models';

export async function createTopic(request: Request, response: Response): Promise<void> {
  try {
    ForumTopic.create({ user: (global as any).user.login, ...request.body })
      .then((topic) => {
        response.status(200).json(topic);
      })
      .catch((ex) => {
        response.status(500).json(ex);
      });
  } catch (ex) {
    response.status(500).json(ex);
  }
}

export async function getTopics(request: Request, response: Response): Promise<void> {
  try {
    ForumTopic.findAll({
      include: [
        {
          model: ForumComment,
          include: [
            {
              model: ForumComment,
            },
          ],
        },
      ],
    })
      .then((topics) => {
        response.status(200).json(topics);
      })
      .catch((ex) => {
        response.status(500).json(ex);
      });
  } catch (ex) {
    response.status(500).json(ex);
  }
}

export async function createComment(request: Request, response: Response): Promise<void> {
  try {
    ForumComment.create({ user: (global as any).user.login, ...request.body })
      .then((topic) => {
        response.status(200).json(topic);
      })
      .catch((ex) => {
        response.status(500).json(ex);
      });
  } catch (ex) {
    response.status(500).json(ex);
  }
}

export async function getComments(request: Request, response: Response): Promise<void> {
  try {
    ForumComment.findAll({
      where: {
        topicId: request.query.id,
      },
      include: [
        {
          model: ForumComment,
          include: [
            {
              model: ForumComment,
            },
          ],
        },
      ],
    })
      .then((topics) => {
        response.status(200).json(topics);
      })
      .catch((ex) => {
        response.status(500).json(ex);
      });
  } catch (ex) {
    response.status(500).json(ex);
  }
}