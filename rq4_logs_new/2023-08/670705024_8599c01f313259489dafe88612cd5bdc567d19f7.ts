import axios from "axios";
import { IncTaskState, taskState } from "store/task.slice";

interface ServerResponse {
    status: string;
    msg: string;
    results: number;
    tasks: taskState[]
}

export async function fetchTasks() {
    return axios<ServerResponse>({
        method: "get",
        withCredentials: true,
        url: "http://localhost:3005/api/task"
    })
    .then(result => result.data.tasks)
    .catch(() => [] as taskState[])
}

export async function addTask(data: IncTaskState, category: {name: string, order: number | null, affected: string[]}) {
    return axios({
        method: "post",
        data: {
            ...data,
            category
        },
        withCredentials: true,
        url: "http://localhost:3005/api/task"
    })
}

export async function updateTask(data: IncTaskState) {
    return axios({
        method: "put",
        data: {

        },
        withCredentials: true,
        url: `http://localhost:3005/api/task/${data.id}`
    })
}