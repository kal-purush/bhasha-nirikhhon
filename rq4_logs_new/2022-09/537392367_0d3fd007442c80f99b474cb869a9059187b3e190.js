const { Op } = require("sequelize");

const { Modality } = require('../../database/models');

module.exports = {
  Query: {
    async searchModalities(_, {name}) {
      return Modality.findAll({ 
        where: {
          name: { 
            [Op.substring]: name, 
          }
        }
      });
    },
  },
};