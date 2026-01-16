'use strict';

module.exports = {
  up: function (queryInterface, Sequelize) {
    queryInterface.addColumn(
            'Supermarkets',
            'categoriesImage',
            Sequelize.STRING
        );
    queryInterface.addColumn(
            'Supermarkets',
            'otherImage',
            Sequelize.STRING
        );
  },

  down: function (queryInterface, Sequelize) {
    /*
      Add reverting commands here.
      Return a promise to correctly handle asynchronicity.

      Example:
      return queryInterface.dropTable('users');
    */
  }
};