import { IUser } from '../store/user';
import Axios from '../utilites/axios';
import { AxiosResponse } from 'axios';
import { VKLoginResult } from 'react-native-vkontakte-login';

export interface IGroupItem {
    id: number,
    name: string,
    photo_50: string | undefined
}

export default class apiVk {
    public static async save(data: VKLoginResult): Promise<AxiosResponse<IUser>> {
        return Axios.post('vk/save', data);
    }

    public static async groups(): Promise<AxiosResponse<IGroupItem[]>> {
        return Axios.get('vk/groups');
    }

    public static async groupsStore(groups: number[]): Promise<AxiosResponse<IUser>> {
        return Axios.post('vk/groups/store', groups);
    }
}