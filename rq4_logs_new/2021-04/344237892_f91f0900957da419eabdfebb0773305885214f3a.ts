import { Sequelize, DataTypes, Model, Optional } from 'sequelize';

import { PaymentDto } from './interfaces/payment.interface';

export type PaymentCreationAttributes = Optional<PaymentDto, 'id' | 'account_id' | 'debit' | 'credit' | 'description'>;

export class PaymentModel extends Model<PaymentDto, PaymentCreationAttributes> implements PaymentDto {
  public id: string;
  public account_id: string;
  public debit: number;
  public credit: number;
  public description: string;

  public readonly createdAt!: Date;
  public readonly updatedAt!: Date;

  static associate(models) {
    this.belongsTo(models.Accounts, { foreignKey: 'account_id' });
  }
}

export default function (sequelize: Sequelize): typeof PaymentModel {
  PaymentModel.init(
    {
      id: {
        primaryKey: true,
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
      },
      account_id: {
        type: DataTypes.UUID,
      },
      debit: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
      },
      credit: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
      },
      description: {
        type: DataTypes.TEXT,
      },
    },
    {
      tableName: 'payments',
      sequelize,
    },
  );

  return PaymentModel;
}