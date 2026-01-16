module.exports = function (sequelize, DataTypes) {
    return sequelize.define('USER', {
        ID: {
            type: DataTypes.INTEGER(11),
            allowNull: false,
            primaryKey: true,
            autoIncrement: true
        },
        username: {
            type: DataTypes.STRING(255),
            allowNull: false,
            unique: true
        },
        email: {
            type: DataTypes.STRING(255),
            allowNull: false,
            unique: true
        },
        password: {
            type: DataTypes.STRING(255),
            allowNull: false
        },

        is_admin: {
            type: DataTypes.INTEGER(1),
            defaultValue: 0,
            allowNull: false
        },
    }, {
            tableName: 'USER'
        });
};