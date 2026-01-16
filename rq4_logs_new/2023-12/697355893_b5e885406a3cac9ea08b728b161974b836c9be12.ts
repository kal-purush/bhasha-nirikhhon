import { Dispatch } from 'redux';

import { fetchTasksRequest, fetchTasksSuccess, fetchTasksFailure } from "@/redux/actions/todoActions";
import api from "@/core/api";

export const fetchUserTasks = () => {
    return async (dispatch: Dispatch) => {
        dispatch(fetchTasksRequest());
        try {
            const { data } = await api.get('/tasks');
            const tasks = data;
            dispatch(fetchTasksSuccess(tasks));
        } catch (error: any) {
            dispatch(fetchTasksFailure(error.response?.data || 'Unknown error'));
        }
    };
};