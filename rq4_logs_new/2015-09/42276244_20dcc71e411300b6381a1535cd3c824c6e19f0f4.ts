/// <reference path="../../typings/q/Q.d.ts" />
/// <reference path="../../typings/mongoose/mongoose.d.ts" />

import * as Mongoose from "mongoose";

export interface IMessageDocument extends Mongoose.Document {
	author: String;
	contents: String;
	timestamp: Number;
}

var messageSchema = new Mongoose.Schema({
	author: String,
	contents: String,
	timestamp: Number
});

export var MessagesModel = Mongoose.model<IMessageDocument>('Messages', messageSchema);