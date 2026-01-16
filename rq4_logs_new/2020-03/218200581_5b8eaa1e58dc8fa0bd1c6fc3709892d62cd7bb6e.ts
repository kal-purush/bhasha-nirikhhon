import axios, { AxiosResponse, AxiosPromise } from 'axios';
import { USER_LABEL, UserProps } from './User';

interface HasId {
    id?: number;
}

export class Sync<T extends HasId> {
    defaultUrl = 'http://localhost:3000/users';

    constructor(public rootUrl) {}

    fetch(data: T): Promise<AxiosResponse> {
        return axios.get(`${this.rootUrl}/${data.id}`);
        //    .then((response: AxiosResponse) => {
        //        this.set(response.data);
        //    });
    }

    save(data: T) {
        const id = data.id;

        if (id) {
            axios.put(`${this.rootUrl}/${id}`, data);
            return;
        }

        axios.post(`${this.rootUrl}`, data);
    }
}