import type { Category, Task } from '@shared/types';
import { CATEGORIES_API_URL, TASKS_API_URL } from '@constants';

async function handleResponse<T>(response: Response): Promise<T> {
	if (!response.ok) {
		throw new Error(`API Error: ${response.status} ${response.statusText}`);
	}
	return response.json();
}

export const categoryApi = {
	getAll: async () => {
		const response = await fetch(`${CATEGORIES_API_URL}`);
		return handleResponse<Category[]>(response);
	},

	getById: async (id: number) => {
		const response = await fetch(`${CATEGORIES_API_URL}/${id}`);
		return handleResponse<Category>(response);
	},

	create: async (data: Omit<Category, 'id'>) => {
		const response = await fetch(`${CATEGORIES_API_URL}`, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(data),
		});
		return handleResponse<Category>(response);
	},

	update: async (id: number, data: Category) => {
		const response = await fetch(`${CATEGORIES_API_URL}/${id}`, {
			method: 'PATCH',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(data),
		});
		return handleResponse<Category>(response);
	},

	delete: async (id: number) => {
		const response = await fetch(`${CATEGORIES_API_URL}/${id}`, {
			method: 'DELETE',
		});
		return handleResponse<Category>(response);
	},
};

export const taskApi = {
	getAll: async () => {
		const response = await fetch(`${TASKS_API_URL}`);
		return handleResponse<Task[]>(response);
	},

	getById: async (id: number) => {
		const response = await fetch(`${TASKS_API_URL}/${id}`);
		return handleResponse<Task>(response);
	},

	getByMonth: async (month: string) => {
		const response = await fetch(`${TASKS_API_URL}?month=${month}`);
		return handleResponse<Task[]>(response);
	},

	create: async (data: Omit<Task, 'id'>) => {
		const response = await fetch(`${TASKS_API_URL}`, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(data),
		});
		return handleResponse<Task>(response);
	},

	update: async (id: number, data: Task) => {
		const response = await fetch(`${TASKS_API_URL}/${id}`, {
			method: 'PATCH',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(data),
		});
		return handleResponse<Task>(response);
	},

	delete: async (id: number) => {
		const response = await fetch(`${TASKS_API_URL}/${id}`, {
			method: 'DELETE',
		});
		return handleResponse<Task>(response);
	},
};