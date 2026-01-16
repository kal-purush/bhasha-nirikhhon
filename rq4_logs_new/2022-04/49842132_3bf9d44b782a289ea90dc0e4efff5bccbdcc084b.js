import * as Const from '../constants';
import List from '../models/List';
import Item from '../models/Item';

class Action {
    constructor() {
        this.dispatch;
    }

    addList() {
        this.dispatch({
            type: Const.ADD_LIST,
            payload: new List(),
        });
    }

    moveList(nextIndex, targetId) {
        this.dispatch({
            type: Const.MOVE_LIST,
            payload: {nextIndex, targetId},
        });
    }

    removeList(targetId) {
        this.dispatch({
            type: Const.REMOVE_LIST,
            payload: {targetId},
        });
    }

    add(listId, data) {
        this.dispatch({
            type: Const.ADD_ITEM,
            payload: {listId, item: new Item(data)},
        });
    }
    update(listId, next) {
        this.dispatch({
            type: Const.UPDATE_ITEM,
            payload: {
                listId,
                next: {
                    targetId: next.targetId,
                    text: next.text,
                    index: next.index,
                },
            },
        });
    }
    del(listId, targetId) {
        this.dispatch({
            type: Const.DELETE_ITEM,
            payload: {listId, targetId},
        });
    }
}

export default new Action();