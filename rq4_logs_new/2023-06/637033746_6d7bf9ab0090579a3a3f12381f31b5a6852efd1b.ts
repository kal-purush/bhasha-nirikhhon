import { Segments } from 'celebrate';
import Joi from 'joi';

export const userSchema = {
  [Segments.BODY]: Joi.object().keys({
    name: Joi.string().min(2).max(30),
    about: Joi.string().min(2).max(200),
    avatar: Joi.string(),
    email: Joi.string().required(),
    password: Joi.string().min(8).required(),
  }),
};

export const updateUserInfoSchema = {
  [Segments.BODY]: Joi.object().keys({
    name: Joi.string().min(2).max(30).required(),
    about: Joi.string().min(2).max(200).required(),
  }),
};

export const updateUserAvatarSchema = {
  [Segments.BODY]: Joi.object().keys({
    avatar: Joi.string().required(),
  }),
};

export const userIdSchema = {
  [Segments.PARAMS]: Joi.object().keys({
    id: Joi.string().required(),
  }),
};

export const loginSchema = {
  [Segments.BODY]: Joi.object().keys({
    email: Joi.string().min(2).max(30).required(),
    password: Joi.string().min(8).required(),
  }),
};