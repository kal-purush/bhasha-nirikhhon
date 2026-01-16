const tableName = 'Users';

module.exports = {
  up: (queryInterface, Sequelize) => {
    return queryInterface.addColumn(tableName, 'imageURL', {
      type: Sequelize.STRING,
      allowNull: true
    });
  },

  /* eslint-disable no-unused-vars */
  down: (queryInterface, Sequelize) => {
    return queryInterface.removeColumn(tableName, 'imageURL');
  }
};