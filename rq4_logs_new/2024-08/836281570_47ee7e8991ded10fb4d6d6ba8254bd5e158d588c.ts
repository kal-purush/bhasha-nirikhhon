import { Model, DataTypes, Optional, Sequelize } from "sequelize";

interface MeetingCacheAttributes {
  id: string;
  key: string;
  result: object;
}

class MeetingCacheInstance
  extends Model<MeetingCacheAttributes>
  implements MeetingCacheAttributes
{
  public id!: string;
  public key!: string;
  public result!: object;
}

const MeetingCacheModel = (sequelize: Sequelize) => {
  MeetingCacheInstance.init(
    {
      id: {
        type: DataTypes.UUID,
        primaryKey: true,
        defaultValue: DataTypes.UUIDV4,
      },
      key: {
        type: DataTypes.STRING,
        unique: true,
        allowNull: false,
      },
      result: {
        type: DataTypes.JSONB,
        allowNull: false,
      },
    },
    {
      sequelize,
      modelName: "MeetingCacheModel",
      timestamps: true,
    }
  );
};

export default MeetingCacheModel;