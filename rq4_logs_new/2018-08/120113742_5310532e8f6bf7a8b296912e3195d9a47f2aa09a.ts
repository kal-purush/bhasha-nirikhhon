
import { Dispatch } from "react-redux";
import { history } from '../store/history';

export const locationChange = (location: string) => async (dispatch: Dispatch<any>) => {
    history.push(location);
};