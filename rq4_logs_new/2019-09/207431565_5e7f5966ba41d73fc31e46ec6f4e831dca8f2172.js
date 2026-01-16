module.exports = {
  up: (queryInterface, Sequelize) => {
    const BankStatements = queryInterface.createTable('BankStatements', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER,
      },
      ammount: {
        allowNull: false,
        type: Sequelize.FLOAT,
      },
      balance: {
        allowNull: false,
        type: Sequelize.FLOAT,
      },
      favoredIdentifier: {
        allowNull: false,
        type: Sequelize.INTEGER,
      },
      AccountID: {
        type: Sequelize.INTEGER,
        references: {
          model: {
            tableName: 'Accounts',
          },
          key: 'id', // key in Target model that we're referencing
        },
      },
      createdAt: {
        allowNull: false,
        type: Sequelize.DATE,
      },
      updatedAt: {
        allowNull: false,
        type: Sequelize.DATE,
        defaultValue: new Date(),
      },
    });

    return BankStatements;
  },

  down: (queryInterface) => queryInterface.dropTable('BankStatements'),
};