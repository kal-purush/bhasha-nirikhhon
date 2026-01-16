import * as Joi from 'joi';

export const SubmissionSchema = Joi.object().keys({
  attribute: Joi.string().max(255).required(),
  value: Joi.string().max(255).required(),
  scope: Joi.string().max(255).required(),
  comments: Joi.string().allow(null),
});