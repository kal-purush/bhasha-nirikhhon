import axios from 'axios';

/*
 * 页面：数据集库
*/

// 返回所有数据集
export function showAllDataset() {
    return axios.post<any>('http://127.0.0.1:7001/datasetAll');
}

// 查找数据集
export function findDataset(data: any) {
    return axios.post<any>('http://127.0.0.1:7001/datasetFind', data);
}

// 数据集详情
export function getDatasetDetail(data: any) {
    return axios.post<any>('http://127.0.0.1:7001/datasetDetail', data);
}

// 添加数据集
export function createDataset(data: any) {
    return axios.post<any>('http://127.0.0.1:7001/datasetCreate', data);
}

// 删除数据集
export function delDataset(data: any) {
    return axios.post<any>('http://127.0.0.1:7001/datasetDelete', data);
}

// 更新数据集信息
export function updateDataset(data: any) {
    return axios.post<any>('http://127.0.0.1:7001/datasetUpdate', data);
}