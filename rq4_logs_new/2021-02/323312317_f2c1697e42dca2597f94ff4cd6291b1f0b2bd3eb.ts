import { startOfToday } from 'date-fns';
import mongoose from 'mongoose';
import { BankInformationModel } from './models/bankinformation/bankinformation.entity';
import { PriceMatrixModel } from './models/pricematrix/pricematrix.entity';
import { UserModel, UserRole, UserStatus } from './models/user/user.entity';
import { hashPassword } from './utils/security';

export interface IDbHandler {
    connect(): Promise<void>
    disconnect(): Promise<void>
}
type MongoOptions = {
    uri: string,
    dbName: string,
    ssl: boolean,
    createDefaults: boolean //TODO this should be done elsewhere
}
export default class DbHandler implements IDbHandler {

    options: MongoOptions;
    constructor(options: MongoOptions) {
        this.options = options;
    }
    async connect(): Promise<void> {
        if (!this.options.uri) {
            console.error('Mongo URI is not set');
            return;
        }
        try {
            await mongoose.connect(this.options.uri, {
                ssl: this.options.ssl,
                dbName: this.options.dbName,
                useNewUrlParser: true,
                useUnifiedTopology: true,
                useFindAndModify: false,
                useCreateIndex: true
            })
            if (this.options.createDefaults) {
                this.setupDefaultAdmin();
                this.setupDefaultPriceMatrix();
                this.setupDefaultBankInformation();
            }
        } catch (error) {
            console.log('Error while connecting to the database', error);
            return error;
        }
    }

    async disconnect(): Promise<void> {
        await mongoose.disconnect();
    }

    async setupDefaultAdmin() {
        const admin = await UserModel.findOne({
            roles: UserRole.admin
        }).exec();
        if (admin) return;
        console.log('creating default admin');
        UserModel.create({
            email: process.env.DEFAULT_ADMIN_EMAIL,
            fullName: process.env.DEFAULT_ADMIN_NAME,
            roles: [UserRole.read, UserRole.basic, UserRole.admin],
            status: UserStatus.approved,
            password: await hashPassword(process.env.DEFAULT_ADMIN_PASSWORD!)
        })
    }

    async setupDefaultPriceMatrix() {
        const existing = await PriceMatrixModel.findOne({}).exec();
        if (existing) return;
        PriceMatrixModel.create({
            validFrom: startOfToday(),
            price: 350,
            tubPrice: 50
        })
    }

    async setupDefaultBankInformation() {
        const existing = await BankInformationModel.findOne({}).exec();
        if (existing) return;
        BankInformationModel.create({
            regNo: '1234',
            accountNo: '1234567890'
        })
    }
}