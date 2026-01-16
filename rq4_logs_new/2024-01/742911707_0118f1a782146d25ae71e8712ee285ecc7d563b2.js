"use strict";

module.exports = {
  up: (queryInterface, Sequelize) => {
    return queryInterface.createTable("Pessoas", {
      id: {
        type: Sequelize.STRING,
        primaryKey: true,
        allowNull: false,
      },
      nome: {
        type: Sequelize.STRING(100),
        allowNull: false,
      },
      apelido: {
        type: Sequelize.STRING(32),
        allowNull: false,
        unique: true,
      },
      nascimento: {
        type: Sequelize.STRING(10),
        allowNull: false,
      },
      stack: {
        type: Sequelize.JSON,
        allowNull: true,
      },
    });
  },

  down: (queryInterface, Sequelize) => {
    return queryInterface.dropTable("Pessoas");
  },
};