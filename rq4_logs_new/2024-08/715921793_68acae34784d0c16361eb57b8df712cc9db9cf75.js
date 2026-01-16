"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getCategory = exports.getAllCategories = void 0;
const models_1 = require("../models");
const getAllCategories = async (_req, res) => {
    try {
        const categories = await models_1.Category.find({ state: true })
            .populate('user', 'name');
        res.json({
            categories
        });
    }
    catch (error) {
        res.status(401).json({
            msg: 'Categories not found'
        });
    }
};
exports.getAllCategories = getAllCategories;
const getCategory = async (req, res) => {
    const { id } = req.params;
    try {
        const category = await models_1.Category.findById(id)
            .populate('user', 'name');
        if (category?.state === null) {
            return res.json({
                msg: 'Category not found'
            });
        }
        return res.json({
            category
        });
    }
    catch (error) {
        return res.status(500).json({
            msg: 'You must talk with admin'
        });
    }
};
exports.getCategory = getCategory;