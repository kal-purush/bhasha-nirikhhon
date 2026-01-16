import { Sequelize } from "sequelize";
import { TypesForms } from '../models/TypesFormsModel.js'
import { FormsFields } from '../models/FormsFieldsModel.js'
import { Forms } from '../models/FormsModel.js';
import { Fields } from '../models/FieldsModel.js'
import { PaymentsInfo } from '../models/PaymentsInfoModel.js'
import { Payments } from '../models/PaymentsModel.js'
import { Qr } from '../models/QrModel.js'
import { InfosFields } from '../models/InfoFieldsModel.js'
import { TypesAccounts } from '../models/TypesAccountsModel.js'
import { Accounts } from '../models/AccountsModel.js'
import { Users } from '../models/UsersModel.js'
import { UsersAccounts } from '../models/UsersAccountsModel.js'
import {  Banks } from '../models/BankModel.js'

const db = new Sequelize("b81vxga8g8mgczkchffu", "uukfhv3y7ljxbstc", "sq0Z0y521uwsiSjENA14", {
    port:"3306",
    host: "b81vxga8g8mgczkchffu-mysql.services.clever-cloud.com",
    dialect: "mysql",
    freezeTableName: true
});

db.TypesForms = TypesForms.init(db,Sequelize);
db.FormsFields= FormsFields.init(db,Sequelize);
db.Forms = Forms.init(db, Sequelize);
db.InfosFields = InfosFields.init(db, Sequelize);
db.Fields = Fields.init(db, Sequelize);
db.PaymentsInfo = PaymentsInfo.init(db, Sequelize);
db.Payments = Payments.init(db, Sequelize);
db.Qr = Qr.init(db, Sequelize);
db.TypesAccounts = TypesAccounts.init(db, Sequelize);
db.Accounts = Accounts.init(db,Accounts);
db.Users = Users.init(db, Sequelize);
db.UsersAccounts= UsersAccounts.init(db,Sequelize)
db.Banks = Banks.init(db, Sequelize);
//
db.TypesForms.hasMany(db.Forms, {
    onDelete: "CASCADE"
});
db.Forms.belongsTo(db.TypesForms);
//
db.TypesForms.hasMany(db.FormsFields, {
  onDelete: "CASCADE"
});
db.FormsFields.belongsTo(db.TypesForms);
//
db.Forms.hasMany(db.Qr, {
    onDelete: "CASCADE"
})
db.Qr.belongsTo(db.Forms);
//
db.Fields.hasMany(db.FormsFields, {
    onDelete: "CASCADE"
});
db.FormsFields.belongsTo(db.Fields);
//
db.Fields.hasMany(db.InfosFields, {
  onDelete: "CASCADE"
})
db.InfosFields.belongsTo(db.Fields);
//
db.InfosFields.hasMany(db.PaymentsInfo, {
  onDelete: "CASCADE"
})
db.PaymentsInfo.belongsTo(db.InfosFields);
//
db.Payments.hasMany(db.PaymentsInfo, {
    onDelete: "CASCADE"
})
db.PaymentsInfo.belongsTo(db.Payments);
//
db.Qr.hasMany(db.Payments, {
  onDelete: "CASCADE"
})
db.Payments.belongsTo(db.Qr);
//relaciones de cuentas y usuarios
db.TypesAccounts.hasMany(db.Accounts, {
  onDelete: "CASCADE"
})
db.Accounts.belongsTo(db.TypesAccounts);
//
db.Accounts.hasMany(db.UsersAccounts, {
  onDelete: "CASCADE",
  unique: true
})
db.UsersAccounts.belongsTo(db.Accounts);
//
db.Users.hasMany(db.UsersAccounts, {
  onDelete: "CASCADE"
})
db.UsersAccounts.belongsTo(db.Users);
//
db.UsersAccounts.hasMany(db.Forms, {
  onDelete: "CASCADE"
})
db.Forms.belongsTo(db.UsersAccounts);
//
db.Banks.hasMany(db.Users, {
  onDelete: "CASCADE"
})
db.Users.belongsTo(db.Banks);
export default db;