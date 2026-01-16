import * as z from "zod";
import { object, number, string, TypeOf } from "zod";
import path from "path";

const addCategoryPayload = {
    body: z.object({
        categoryName: z.string().min(1, 'Category name is required').max(255, 'Category name is too long'),
        categoryImage: z.string().url('Category image must be a valid URL'),
    }),
};

const editCategoryPayload = {
    body: z.object({
        categoryName: z.string().min(1, 'Category name is required').max(255, 'Category name is too long').optional(),
        categoryImage: z.string().url('Category image must be a valid URL').optional(),
    }),
};




const params = {
    params: z.object({
        categoryId: z.string().uuid('Category ID must be a valid UUID'),
    }),
};


export const addCategorySchema = z.object({
    ...addCategoryPayload,
});


export const editCategorySchema = z.object({
    ...params,
    ...editCategoryPayload,
});

export const deleteCategorySchema = z.object({
    ...params,
});


export type AddCategorySchema = z.infer<typeof addCategorySchema>;
export type EditCategorySchema = z.infer<typeof editCategorySchema>;
export type DeleteCategorySchema = z.infer<typeof deleteCategorySchema>;