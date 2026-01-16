const BlogCategory = require('../models/blogCategoryModel');
const asyncHander = require('express-async-handler');
const { validateMongoDbId } = require('../utils/validateMongoDbID');

// Get All Categories

const getBlogCategories = asyncHander(async (req, res) => {
    try {
        const categories = await BlogCategory.find();
        res.json(categories);
    } catch (error) {
        throw new Error(error);
    }
})

// Get  Category By ID

const getBlogCategory = asyncHander(async (req, res) => {
    try {
        const { id } = req.params;
        validateMongoDbId(id);
        const category = await BlogCategory.findById(id);
        res.json(category);
    } catch (error) {
        throw new Error(error);
    }
})

// Create New Category

const createBlogCategory = asyncHander(async (req, res) => {
    try {
        const newCategory = await BlogCategory.create(req.body);
        res.json(newCategory);
    } catch (error) {
        throw new Error(error)
    }
})

// Update Blog Category
const updateBlogCategory = asyncHander(async (req, res) => {
    try {
        const { id } = req.params;
        validateMongoDbId(id);
        const updatedCategory = await BlogCategory.findByIdAndUpdate(id, req.body, { new: true });
        res.json(updatedCategory);
    } catch (error) {
        throw new Error(error)
    }
})
// Delete Blog Category
const deleteBlogCategory = asyncHander(async (req, res) => {
    try {
        const { id } = req.params;
        validateMongoDbId(id);
        const deleteCategory = await BlogCategory.findByIdAndDelete(id);
        res.json(deleteCategory);
    } catch (error) {
        throw new Error(error)
    }
})

module.exports = {
    getBlogCategories,
    getBlogCategory,
    createBlogCategory,
    updateBlogCategory,
    deleteBlogCategory,
}