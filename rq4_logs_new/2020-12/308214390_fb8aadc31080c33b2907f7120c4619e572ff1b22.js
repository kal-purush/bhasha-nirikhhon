'use strict';
module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('Users', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      name: {
        type: Sequelize.STRING
      },
      mailaddress: {
        type: Sequelize.STRING
      },
      password: {
        type: Sequelize.STRING
      },
      wheretogo: {
        type: Sequelize.STRING
      },
      go_date: {
        allowNull: false,
        type: Sequelize.DATE
      },
      go_time: {
        allowNull: false,
        type: Sequelize.TIME
      },
      back_date: {
        allowNull: false,
        type: Sequelize.DATE
      },
      back_time: {
        allowNull: false,
        type: Sequelize.TIME
      },
      active: {
        type: Sequelize.BOOLEAN
      },
      createdAt: {
        allowNull: false,
        type: Sequelize.DATE
      },
      updatedAt: {
        allowNull: false,
        type: Sequelize.DATE
      }
    });
  },
  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('Users');
  }
};