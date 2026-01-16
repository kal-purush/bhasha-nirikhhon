import { connect as connectRedux } from 'react-redux';
import { bindActionCreators } from 'redux';

import * as actions from 'actions/actionCreators';
import { ILayer, ISize, IState } from 'types/state';
import { withStyles } from 'material-ui/styles';
import { TextField } from 'material-ui';
import { ActionCreators } from 'redux-undo';
import { store } from 'index';



interface IUndoableState {
    present: IState;
}

/**
 * Undoable なステートを connect して withStyles を適用する
 */
export default function connect(component: any, c: any, styles: any) {

    return connectRedux((undoableState: IUndoableState) => {

        const state = undoableState.present;

        console.warn('Connect: ', state);

        return c(state, undoableState);

    }, (dispatch) => ({
        actions: bindActionCreators(actions, dispatch)
    }))(withStyles(styles)(component));

}