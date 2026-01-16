import {Card as CardModel} from "./models/card/Card.models";
import {CardOrder as CardOrderModel} from "./models/cardOrdering/CardOrder.model";
import {CardTrade as CardTradeModel} from "./models/cardTrade/CardTrade.model";
import {Trader as TraderModel} from "./models/trader/Trader.model";
import {IdentityUser as IdentityUserModel} from "./models/identityUser/IdentityUser.model";
import {Sequelize} from "sequelize-typescript";

export interface DatabaseCredential {
    username?: string;
    password?: string;
    host?: string;
    database?: string;
    port?: number
}

export interface Models{
    cardModel: typeof CardModel
    cardOrderModel: typeof CardOrderModel;
    cardTradeModel: typeof CardTradeModel;
    traderModel: typeof TraderModel;
    identityUserModel: typeof IdentityUserModel;
}

export class CardPlatformSequel {
    private readonly sequelizeInstance?: Sequelize;

    private constructor(config: DatabaseCredential) {
        this.sequelizeInstance = new Sequelize({
            database: config.database,
            username: config.username,
            password: config.password,
            host: config.host,
            dialect: 'mysql',
            port: config.port ? config.port : 3306,
            dialectOptions: {
                multipleStatements: true,
            },
            pool: {
                max: 5,
                min: 0,
                idle: 10000
            },
            logging: false
        });
        this.sequelizeInstance.addModels([CardModel, CardOrderModel, CardTradeModel, TraderModel, IdentityUserModel])
    }

    get sequelInstance(): Sequelize {
        return this.sequelizeInstance
    }

    get models(): Models {
        return {
            cardModel: CardModel,
            cardOrderModel:CardOrderModel,
            cardTradeModel:CardTradeModel,
            traderModel:TraderModel,
            identityUserModel: IdentityUserModel,
        }
    }

    authConnection(): void {
        try {
            this.sequelizeInstance.authenticate();
            console.log('Connection has been established successfully.');
        } catch (error) {
            console.error('Unable to connect to the database:', error);
        }
    }

    public static create(config: DatabaseCredential): CardPlatformSequel {
        return new CardPlatformSequel(config)
    }

}