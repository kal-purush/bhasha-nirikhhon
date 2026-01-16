import {createAction} from "redux-actions";
import * as types from "../constants/ActionTypes";
import {Tweet} from "../entities/Tweet";

const toggleTopbar: any = createAction(
    types.TOGGLE_TOPBAR
);
const toggleSidebar: any = createAction(
    types.TOGGLE_SIDEBAR
);
export {
    toggleSidebar,
    toggleTopbar
}