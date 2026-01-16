import { axiosInstance } from './axios';

export const addTodo = async (todo: string) => {
	try {
		const response = await axiosInstance.post('/todos', { todo });
		return response;
	} catch (error: any) {
		return error.response;
	}
};

interface TodoProps {
	todo: {
		todo: string;
		isCompleted: boolean;
	};
	id: number;
}

export const updateTodo = async ({ todo, id }: TodoProps) => {
	try {
		const response = await axiosInstance.put(`/todos/${id}`, { ...todo });
		return response;
	} catch (error: any) {
		return error.response;
	}
};
export const deleteTodo = async (id: number) => {
	try {
		const response = await axiosInstance.delete(`/todos/${id}`);
		return response;
	} catch (error: any) {
		return error.response;
	}
};

export const getTodos = async () => {
	try {
		const response = await axiosInstance.get('/todos');
		return response;
	} catch (error: any) {
		return error.response;
	}
};