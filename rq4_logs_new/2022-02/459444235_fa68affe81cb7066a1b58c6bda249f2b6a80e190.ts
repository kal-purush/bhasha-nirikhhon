// api
import axios from "axios"
import { Todo } from "../components/types/Types"

// モックサーバのURL
const todoUrl: string = "http://localhost:3100/todos"

// 全件取得
export const getAllTodoData = async (): Promise<object> => {
    const response = await axios.get(todoUrl)
    return response.data
}

// 新規追加
export const addTodoData = async (todo: Todo): Promise<object> => {
    const response = await axios.post(todoUrl, todo)
    return response.data
}

// 削除
export const deleteTodoData = async (id: string): Promise<string> => {
    await axios.delete(`${todoUrl}/${id}`)
    return id
}

// 更新
export const updateTodoData = async (id: string, todo: Todo): Promise<object> => {
    const response = await axios.put(`${todoUrl}/${id}`, todo)
    return response.data
}