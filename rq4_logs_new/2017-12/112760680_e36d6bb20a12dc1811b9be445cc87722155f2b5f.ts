import { IState } from "types/state";
import { IAction } from "types/actions";


export default function reducer(state: IState, action: IAction<any>): IState {


    switch (action.type) {
        case 'BACKGROUND_UPLOAD_SUCCESS':

            console.warn('BACKGROUND_UPLOAD_SUCCESS');

            return Object.assign(state, {

            });
    }


    return null;
}