import {createSelector} from 'reselect';
import LogState from "../store/state/LogState";
import {useDispatch} from "react-redux";
import {error, info, success, warn} from "../store/actions/log/log";

interface ILogState {
    log: LogState
}

export const getLogState = createSelector(
    (state: ILogState) => {
        return state.log;
    },
    log => log
);

export function useInfo() {
    const dispatch = useDispatch();
    return (text: string) => dispatch(info(text));
}

export function useWarn() {
    const dispatch = useDispatch();
    return (text: string) => dispatch(warn(text));
}

export function useError() {
    const dispatch = useDispatch();
    return (text: string) => dispatch(error(text));
}

export function useSuccess() {
    const dispatch = useDispatch();
    return (text: string) => dispatch(success(text));
}