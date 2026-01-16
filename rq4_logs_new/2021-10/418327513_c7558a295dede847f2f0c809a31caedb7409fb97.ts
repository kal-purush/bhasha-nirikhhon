import {Table, Column, Model, DataType, HasMany} from 'sequelize-typescript';
import {CardOrder} from "./CardOrder.model";
import {CardTrade} from "./CardTrade.model";

interface CardAttributes {
    card_id: number;
    card_name: string;
    created_time?: Date;
    updated_time?: Date;
}

@Table({
    timestamps: true,
    tableName: 'card',
    underscored: true,
    indexes: [{
        unique: true,
        fields: ['card_name']
    }],
    createdAt: 'created_time',
    updatedAt: 'updated_time'
})
export class Card extends Model<CardAttributes> {
    @Column({
        type: DataType.INTEGER,
        autoIncrement: true,
        allowNull: false,
        primaryKey: true
    })
    public card_id!: number;

    @Column({
        type: DataType.STRING(250),
        allowNull: false,
        unique: true
    })
    public card_name!: string;

    public readonly created_time!: Date;
    public readonly updated_time!: Date;

    @HasMany(() => CardOrder,
        {foreignKey: 'order_card_id', sourceKey: 'card_id', as: 'CardOrder'})
    public cardOrders: CardOrder[]


    @HasMany(() => CardTrade,
        {foreignKey: 'trade_card_id', sourceKey: 'card_id', as: 'CardTrade'})
    public cardTrades: CardTrade[]
}