import { FakePostItem } from './types';
import axios from 'axios';

const endpoint = 'https://jsonplaceholder.typicode.com/posts';

export const FakeGetListApi = (): Promise<FakePostItem[]> => {
    return axios.get(endpoint);
}

export const FakeGetItemApi = (params): Promise<FakePostItem> => {
    return axios.get(endpoint, { params });
}

export const FakeDeleteItemApi = (params): Promise<FakePostItem> => {
    return axios.delete(endpoint, { params });
}

export const FakeUpdateItemApi = (params, body): Promise<FakePostItem> => {
    return axios.put(endpoint, body, { params });
}

export const FakeAddItemApi = (body):Promise<FakePostItem> => {
    return axios.post(endpoint, body);
}