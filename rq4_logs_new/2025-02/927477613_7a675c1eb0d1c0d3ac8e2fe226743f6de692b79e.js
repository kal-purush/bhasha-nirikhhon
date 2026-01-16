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
Object.defineProperty(exports, "__esModule", { value: true });
exports.deleteUser = exports.updateUser = exports.getUser = exports.addUser = void 0;
const mongodb = require('../mongodb/connect');
const { ObjectId } = require('mongodb');
const addUser = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { name, email, username, password } = req.body;
    const newUser = { name, email, username, password };
    const db = mongodb.getDB();
    const result = yield db.collection('users').insertOne(newUser);
    if (result.acknowledged) {
        res.status(201).json({ message: 'User created successfully' });
    }
    else {
        res.status(500).json({ message: 'Failed to create user' });
    }
});
exports.addUser = addUser;
const getUser = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        let id = req.params.id;
        const userId = new ObjectId(id);
        const db = mongodb.getDB();
        const result = yield db.collection('users').findOne({ _id: userId });
        if (!result) {
            return res.status(404).json({ message: 'User not found' });
        }
        res.setHeader('Content-Type', 'application/json');
        res.status(200).json(result);
    }
    catch (error) {
        res.status(404).json({ message: 'User not found', error });
        console.error(`Error: `, error);
    }
});
exports.getUser = getUser;
const updateUser = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { id } = req.params;
    const { name, email, username, password } = req.body;
    const db = mongodb.getDB();
    const result = yield db.collection('users').updateOne({ _id: new ObjectId(id) }, { $set: { name, email, username, password } });
    if (result.acknowledged) {
        res.status(200).json({ message: 'User updated successfully' });
    }
    else {
        res.status(500).json({ message: 'Failed to update user' });
    }
});
exports.updateUser = updateUser;
const deleteUser = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { id } = req.params;
    const db = mongodb.getDB();
    const result = yield db.collection('users').deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount) {
        res.status(200).json({ message: 'User deleted successfully' });
    }
    else {
        res.status(500).json({ message: 'Failed to delete user' });
    }
});
exports.deleteUser = deleteUser;