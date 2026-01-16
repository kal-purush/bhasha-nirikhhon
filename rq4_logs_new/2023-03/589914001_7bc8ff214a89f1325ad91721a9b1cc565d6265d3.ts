import {
  Model,
  DataTypes,
  InferAttributes,
  InferCreationAttributes,
  CreationOptional,
} from 'sequelize';

import { sequelize } from '../util/db';

class Configuration extends Model
  <InferAttributes<Configuration>,
  InferCreationAttributes<Configuration>> {
  declare id: CreationOptional<number>;

  declare daysToStart: number;

  declare maxDaysInFuture: number;
}

Configuration.init(
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    daysToStart: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    maxDaysInFuture: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
  },
  {
    sequelize,
    underscored: true,
    timestamps: true,
    modelName: 'configuration',
  },
);

export default Configuration;