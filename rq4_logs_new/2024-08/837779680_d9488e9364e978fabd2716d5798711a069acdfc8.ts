import { DataTypes, Model } from "sequelize";
import sequelize from "../database";

class Address extends Model {}

Address.init(
    {
        id: {
            type: DataTypes.INTEGER,
            primaryKey: true
        },
        city: {
            type: DataTypes.STRING(50),
            allowNull: false,
            unique: true
        },       
        street: {
            type: DataTypes.STRING(50),
            allowNull: false
        },
        buildingNumber: {
            type: DataTypes.INTEGER,
            allowNull: false
        }
    },
    {
        sequelize,
        modelName: "Address",
        tableName: "addresses",
        timestamps: false
    }
);