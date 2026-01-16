'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class StudentCourses extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      // define association here
    }
  }
  StudentCourses.init({
    student_id: {
      type:DataTypes.INTEGER,
      primaryKey: true
    },
    course_id: {
      type:DataTypes.INTEGER,
      primaryKey: true
    }
  }, {
    sequelize,
    modelName: 'StudentCourses',
    tableName: 'student_courses',
    timestamps: false
  });
  return StudentCourses;
};