const Boom =require('boom');
const Joi = require('joi');

const UsersModel = require('../../models/users');

const user = {
  list: {
    tags: ['api'],
    handler: async (request, reply) => {
      console.info(request.query);
      let user;
      try {
        user = await UsersModel.find();
      } catch (error) {
        request.log.error(error);
        reply(Boom.notFound(error.message));
      }
      reply({ user });
    },
  },
  get: {
    tags: ['api'],
    validate: {
      params: {
        id: Joi.array(),
      },
    },
    handler: async (request, reply) => {
      const { id } = request.params;
      let user;
      try {
        user = await UsersModel.findById(id);
      } catch (error) {
        request.log.error(error);
        reply(Boom.notFound(error.message));
      }
      reply({ user });
    },
  },
  create: {
    tags: ['api'],
    validate: {
      payload: {
        name: Joi.string(),
        passwd: Joi.string(),
        tags: Joi.array(),
      },
    },
    handler: async (request, reply) => {
      const {
        payload: {
          name, passwd, tags,
        },
      } = request;
      const list = new UsersModel({ name, passwd, tags });
      let user;
      try {
        user = await list.save();
      } catch (error) {
        request.log.error(error);
        reply(Boom.badRequest(error.message));
      }
      reply({ user }).code(201);
    },
  },
  update: {
    tags: ['api'],
    validate: {
      params: {
        id: Joi.string(),
      },
    },
    handler: async (request, reply) => {
      const {
        payload: {
          name, passwd, tags,
        },
        params: {
          id,
        },
      } = request;
      let user;
      try {
        user = await  UsersModel.where({ _id: id }).update({ name, passwd, tags });
      } catch (error) {
        request.log.error(error);
        reply(Boom.badRequest(error.message));
      }
      reply({ user });
    },
  },
  destroy: {
    tags: ['api'],
    validate: {
      params: {
        id: Joi.string(),
      },
    },
    handler: async (request, reply) => {
      const { params: { id } } = request;
      let user;
      try {
        user = await UsersModel.remove({ _id: id });
      } catch (error) {
        request.log.error(error);
        reply(Boom.badRequest(error.message));
      }
      reply({ user }).code(204);
    },
  },
};

module.exports = user;