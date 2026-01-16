import { useMutation } from "react-query";
import { AuthService } from "../services";
import { AnyAction } from "redux";
import { RootState } from "../types";
import { ThunkDispatch } from "redux-thunk";
import { NavigateFunction } from "react-router-dom";

const useLogout = () => {
    return useMutation(
        (
            {
                dispatch,
                navigate
            }: {
                dispatch: ThunkDispatch<RootState, unknown, AnyAction>,
                navigate: NavigateFunction
            }
        ) => AuthService.logout(dispatch, navigate)
    );
}

export default useLogout;