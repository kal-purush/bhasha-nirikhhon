import {Player} from "./Player";

/**
 * Only give out a card or skip. (Currently)
 */
interface IAction {

}

interface IDetailedAction {

}

export class DetaidAction implements IDetailedAction {

}

export class Action implements IAction {

    private _owner: Player;
    private _action: IDetailedAction;

    constructor(json: Object) {
        //this._owner = json.owner;
        //
        //this._action = json.action;
    }

    get owner(): Player {
        return this._owner;
    }
}