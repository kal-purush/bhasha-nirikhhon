const Sequelize = require("sequelize");
module.exports = function (sequelize, DataTypes) {
	const Quiz = sequelize.define(
		"quiz",
		{
			quiz_id: {
				autoIncrement: true,
				type: DataTypes.BIGINT,
				allowNull: false,
				primaryKey: true,
			},
			question: {
				type: DataTypes.TEXT,
				allowNull: true,
			},
			correct_answer: {
				type: DataTypes.BOOLEAN,
				allowNull: true,
				defaultValue: false,
			},
		},
		{
			sequelize,
			tableName: "quiz",
			timestamps: false,

			indexes: [
				{
					name: "PRIMARY",
					unique: true,
					using: "BTREE",
					fields: [{ name: "quiz_id" }],
				},
			],
		},
	);

	return Quiz;
};