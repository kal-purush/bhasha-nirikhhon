"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var mongoose_1 = __importDefault(require("mongoose"));
//Data schema
var invoiceSchema = new mongoose_1.default.Schema({
    id: {
        type: String,
        unique: true,
    },
    name: String,
    address: String,
    invoice_number: Number,
    invoice_file: String,
    date: {
        type: Date,
        default: Date.now()
    }
});
module.exports = mongoose_1.default.model("invoices", invoiceSchema);