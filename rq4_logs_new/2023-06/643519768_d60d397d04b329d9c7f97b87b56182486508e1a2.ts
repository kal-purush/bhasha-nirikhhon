import { ICategory } from '@/types/category.interface';

import { instance } from '@/api/api.interceptor';

export const CategoryService = {
	async getAll() {
		return await instance<ICategory[]>({
			url: `/categories`,
			method: 'GET',
		});
	},

	async getById(id: string) {
		return await instance<ICategory>({
			url: `/categories/${id}`,
			method: 'GET',
		});
	},

	async getBySlug(id: string) {
		return await instance<ICategory>({
			url: `/categories/by-slug/${id}`,
			method: 'GET',
		});
	},

	async create() {
		return await instance<ICategory>({
			url: `/categories`,
			method: 'POST',
		});
	},

	async update(id: string, categoryName: string) {
		return await instance<ICategory>({
			url: `/categories/${id}`,
			method: 'PUT',
			data: { name: categoryName },
		});
	},

	async delete(id: string) {
		return await instance<ICategory>({
			url: `/categories/${id}`,
			method: 'DELETE',
		});
	},
};

export default CategoryService;