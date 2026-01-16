export interface Action {
    scope : any;
    func : Function;
}

export class ActionMap {
    private static _intance : ActionMap = null;
    private _actions : Map<string, Action>;
    private lastIndex : number;
    private hostPrefix : string = "ckhost";

    public static get instance() : ActionMap {
        if(ActionMap._intance === null) {
            ActionMap._intance = new ActionMap();
        }

        return ActionMap._intance;
    }

    public get actions() : Map<string, Action> {
        return this._actions;
    }

    constructor() {
        this._actions = new Map<string, Action>();
        this.lastIndex = 0;
    }

    public regiterAction(action : Action) : string {
        this.lastIndex++;
        let hostAttr : string = this.hostPrefix + this.lastIndex;
        this._actions.set(hostAttr, action);
        return hostAttr;
    }

    public unregisterAction(hostAttr : string) {
        this._actions.delete(hostAttr);
    }

    public getAction(hostAttr : string) {
        return this._actions.get(hostAttr);
    }
}