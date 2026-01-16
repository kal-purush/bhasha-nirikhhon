import { Request, Response } from 'express';
import { addCategoryService, deleteCategoryService, editCategoryService } from '../services/category.service.js';

// Add a new category
export const addCategoryHandler = async (req: Request, res: Response)=> {
        // Logic to add category (e.g., save to database)
        const response = await addCategoryService(req);

        return res.send(response);

};

// Delete a category
export const deleteCategoryHandler = async (req: Request, res: Response) => {
    const categoryId = req.params.categoryId;
    if (!categoryId) {
        return res.status(400).send({ error: 'Category ID is required' });
    }
    const response = await deleteCategoryService({ params: { categoryId } });
    return res.status(200).send(response);
};

// Edit a category
export const editCategoryHandler = async (req: Request, res: Response) => {
    const { categoryId } = req.params;
    const { categoryName, categoryImage } = req.body;

    if (!categoryId) {
        return res.status(400).send({ error: 'Category ID is required' });
    }

    const response = await editCategoryService({
        params: { categoryId },
        body: { categoryName, categoryImage },
    });
    return res.status(200).send(response);
};