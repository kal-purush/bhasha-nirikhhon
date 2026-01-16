import { Express } from 'express';
import { ResLocals } from '../types';
import { API_PATH } from '../router/paths';
import { auth } from '../middlewares/auth';
import { StringifyKeys } from '@common/types';
import { ForumModel } from '../models/ForumModel';
import {
  CreateForumMessageParams,
  CreateForumTopicParams,
  GetForumMessagesParams,
} from '@api/schema';

export const forumApi = (app: Express) => {
  app.get(API_PATH.FORUM_TOPICS, auth, (_req, res) => {
    (async () => {
      const topics = await ForumModel.getTopics();

      res.status(200).json(topics);
    })();
  });

  app.post<any, any, CreateForumTopicParams, any, ResLocals>(
    API_PATH.FORUM_TOPICS,
    auth,
    (req, res) => {
      const { title } = req.body;
      const userId = res.locals.user.id;

      (async () => {
        const result = await ForumModel.createTopic({
          userId,
          title,
        });

        res.status(200).json(result);
      })();
    }
  );

  app.get<any, any, any, StringifyKeys<GetForumMessagesParams>>(
    API_PATH.FORUM_MESSAGES,
    auth,
    (req, res) => {
      (async () => {
        const topicId = Number(req.query.topicId);

        const messages = await ForumModel.getMessages({ topicId });

        res.status(200).json(messages);
      })();
    }
  );

  app.post<any, any, CreateForumMessageParams>(
    API_PATH.FORUM_MESSAGES,
    auth,
    (req, res) => {
      (async () => {
        const { topicId, text, parentId } = req.body;
        const userId = res.locals.user.id;

        const result = await ForumModel.createMessage({
          text,
          topicId,
          userId,
          parentId,
        });

        res.status(200).json(result);
      })();
    }
  );
};