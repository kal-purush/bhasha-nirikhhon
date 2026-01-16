import { useDispatch } from "react-redux";
import { useMemo } from "react";
import { bindActionCreators } from "@reduxjs/toolkit";
import { actions as userActions } from "../service/store/slices/user.slice";
import { actions as favoritesActions} from "../service/store/slices/favorites.slice";
import { actions as watchedActions } from "../service/store/slices/watched.slice";

const rootActions = {
    ...userActions,
    ...favoritesActions,
    ...watchedActions
}

export const useActions = () => {
    const dispatch = useDispatch();
    
    return useMemo(() =>
        bindActionCreators(rootActions, dispatch), [dispatch])
}