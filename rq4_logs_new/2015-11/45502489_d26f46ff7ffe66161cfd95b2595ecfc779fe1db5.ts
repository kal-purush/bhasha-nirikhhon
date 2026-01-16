import {ACCOUNT_ACTIONS} from '../action-constants';

export class AccountActions {

    static $inject = ['dispatcher'];

    constructor(
        private dispatcher: Rx.Subject<any>) { }

    getAccount(id, token) {
        this.dispatcher.onNext({
            actionType: ACCOUNT_ACTIONS.GET_ACCOUNT,
            id: id,
            token: token
        });
    }

}