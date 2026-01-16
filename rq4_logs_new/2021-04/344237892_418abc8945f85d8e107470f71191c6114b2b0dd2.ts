import { Sequelize, DataTypes, Model, Optional } from 'sequelize';

import { ErrorDto } from './interfaces/error.interface';

export type TaskCreationAttributes = Optional<ErrorDto, 'id' | 'event_id' | 'event_version' | 'event_name' | 'event_time' | 'producer' | 'message'>;

export class ErrorModel extends Model<ErrorDto, TaskCreationAttributes> implements ErrorDto {
  public id: string;
  public event_id: string;
  public event_version: string;
  public event_name: string;
  public event_time: string;
  public producer: string;
  public message: string;

  public readonly createdAt!: Date;
  public readonly updatedAt!: Date;
}

export default function (sequelize: Sequelize): typeof ErrorModel {
  ErrorModel.init(
    {
      id: {
        primaryKey: true,
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
      },
      event_id: {
        primaryKey: true,
        type: DataTypes.UUID,
        allowNull: true,
      },
      event_version: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      event_name: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      event_time: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      producer: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      message: {
        type: DataTypes.TEXT,
        allowNull: false,
      },
    },
    {
      tableName: 'errors',
      sequelize,
    },
  );

  return ErrorModel;
}