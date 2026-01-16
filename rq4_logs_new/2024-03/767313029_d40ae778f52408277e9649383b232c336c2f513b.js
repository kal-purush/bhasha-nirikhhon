#!/usr/bin/env node
"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
var inquirer_1 = require("inquirer");
var faker_1 = require("@faker-js/faker");
var chalk_1 = require("chalk");
var Customer = /** @class */ (function () {
    function Customer(firstName, lastName, age, gender, mobNum, accNum) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.gender = gender;
        this.mobNum = mobNum;
        this.accNum = accNum;
    }
    return Customer;
}());
var Bank = /** @class */ (function () {
    function Bank() {
        this.customers = [];
        this.account = [];
    }
    Bank.prototype.addCustomer = function (obj) {
        this.customers.push(obj);
    };
    Bank.prototype.addAccountNo = function (obj) {
        this.account.push(obj);
    };
    Bank.prototype.transaction = function (obj) {
        var newAccount = this.account.filter(function (acc) { return acc.accNum !== obj.accNum; });
        this.account = __spreadArray(__spreadArray([], newAccount, true), [obj], false);
    };
    return Bank;
}());
var myBank = new Bank();
for (var i = 1; i <= 3; i++) {
    var fName = faker_1.faker.person.firstName();
    var lName = faker_1.faker.person.lastName();
    var num = parseInt(faker_1.faker.phone.number("923#########"));
    var customer = new Customer(fName, lName, 20 * i, "female", num, 1000 + i);
    myBank.addCustomer(customer);
    myBank.addAccountNo({ accNum: customer.accNum, balance: 100000 * i });
}
function bankService(bank) {
    return __awaiter(this, void 0, void 0, function () {
        var _loop_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _loop_1 = function () {
                        var service, res_1, account_1, name_1, res_2, account, ans, newBalance, res_3, account, ans, newBalance;
                        return __generator(this, function (_b) {
                            switch (_b.label) {
                                case 0: return [4 /*yield*/, inquirer_1.default.prompt({
                                        type: "list",
                                        name: "select",
                                        message: "Please select the services:",
                                        choices: ["Balance", "Withdraw", "Deposit"]
                                    })];
                                case 1:
                                    service = _b.sent();
                                    if (!(service.select == "Balance")) return [3 /*break*/, 3];
                                    return [4 /*yield*/, inquirer_1.default.prompt({
                                            type: "input",
                                            name: "number",
                                            message: "Enter your account number"
                                        })];
                                case 2:
                                    res_1 = _b.sent();
                                    account_1 = myBank.account.find(function (acc) { return acc.accNum == res_1.number; });
                                    if (!account_1) {
                                        console.log(chalk_1.default.red.bold("Invalid account number!"));
                                    }
                                    if (account_1) {
                                        name_1 = myBank.customers.find(function (val) { return val.accNum == (account_1 === null || account_1 === void 0 ? void 0 : account_1.accNum); });
                                        console.log("Dear ".concat(chalk_1.default.green.bold((name_1 === null || name_1 === void 0 ? void 0 : name_1.firstName) + " " + (name_1 === null || name_1 === void 0 ? void 0 : name_1.lastName)), "! Your account balance is Rs ").concat(chalk_1.default.blue.bold(account_1.balance)));
                                    }
                                    _b.label = 3;
                                case 3:
                                    if (!(service.select == "Withdraw")) return [3 /*break*/, 6];
                                    return [4 /*yield*/, inquirer_1.default.prompt({
                                            type: "input",
                                            name: "number",
                                            message: "Enter your account number"
                                        })];
                                case 4:
                                    res_2 = _b.sent();
                                    account = myBank.account.find(function (acc) { return acc.accNum == res_2.number; });
                                    if (!account) {
                                        console.log(chalk_1.default.red.bold("Invalid account number!"));
                                    }
                                    if (!account) return [3 /*break*/, 6];
                                    return [4 /*yield*/, inquirer_1.default.prompt({
                                            type: "number",
                                            name: "rupee",
                                            message: "Enter your withdrawl amount:"
                                        })];
                                case 5:
                                    ans = _b.sent();
                                    if (ans.rupee > account.balance) {
                                        console.log(chalk_1.default.red.bold("You have insufficient balance"));
                                    }
                                    newBalance = account.balance - ans.rupee;
                                    bank.transaction({ accNum: account.accNum, balance: newBalance });
                                    _b.label = 6;
                                case 6:
                                    if (!(service.select == "Deposit")) return [3 /*break*/, 9];
                                    return [4 /*yield*/, inquirer_1.default.prompt({
                                            type: "input",
                                            name: "number",
                                            message: "Enter your account number"
                                        })];
                                case 7:
                                    res_3 = _b.sent();
                                    account = myBank.account.find(function (acc) { return acc.accNum == res_3.number; });
                                    if (!account) {
                                        console.log(chalk_1.default.red.bold("Invalid account number!"));
                                    }
                                    if (!account) return [3 /*break*/, 9];
                                    return [4 /*yield*/, inquirer_1.default.prompt({
                                            type: "number",
                                            name: "rupee",
                                            message: "Enter your withdrawl amount:"
                                        })];
                                case 8:
                                    ans = _b.sent();
                                    newBalance = account.balance + ans.rupee;
                                    bank.transaction({ accNum: account.accNum, balance: newBalance });
                                    _b.label = 9;
                                case 9: return [2 /*return*/];
                            }
                        });
                    };
                    _a.label = 1;
                case 1: return [5 /*yield**/, _loop_1()];
                case 2:
                    _a.sent();
                    _a.label = 3;
                case 3:
                    if (true) return [3 /*break*/, 1];
                    _a.label = 4;
                case 4: return [2 /*return*/];
            }
        });
    });
}
bankService(myBank);