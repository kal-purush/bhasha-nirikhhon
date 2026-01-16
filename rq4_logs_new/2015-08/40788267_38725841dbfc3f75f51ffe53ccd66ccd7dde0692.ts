
///<reference path="Task.ts"/>



class ActionHelper {

	static getChain(action: IAction): Array<IAction> {
		if (action.parent === null)
    		return [action];
    	else {
    		var result : Array<IAction> = ActionHelper.getChain(action.parent);
    		result.push(action);
    		return result;
    	}
	}

}



/*
 *  IAction
 */
interface IAction extends ITask {
	
	getNextAction(): IAction;
	
	isComplex(): boolean;
	
	isExecutable(): boolean;
	
	isEmpty(): boolean;

	parent: IAction;
}





class Action implements IAction {

	private _parent: IAction = null;
	
	private _worker: ITaskWorker = null;

	
	constructor (parent: IAction, worker: ITaskWorker) {
		this._parent = parent;
        this._worker = worker;
	}


    applyResult(result: ITaskResult) : void {}
	
	getNextAction()  : IAction     {return null}
	
	isExecutable()   : boolean     {return true}
	
	isComplex()      : boolean     {return false}
	
	get parent()     : IAction     {return this._parent}
	
	get worker()     : ITaskWorker {return this._worker}
	
	isEmpty()        : boolean     {return this._worker === null}
	
    toString()       : string      {return this.constructor['name']}
}




/*
 *  ActionList
 */	
class ActionList extends Action {
	
	actionList : Array<IAction> = [];
	
	constructor (parent: IAction) {
		super(parent, null);
	}
	
	
	/**
	 *  Return actions in natural order
	 */
	getNextAction(): IAction {
		return this.actionList.shift() || null;
	}
	
	
    push(action: IAction): void {
        if (action != null)
            this.actionList.push(action);
    }

	
    pushMany(actions: Array<IAction>): void {
        if (actions != null)
            for (var i = 0; i < actions.length; i++) {
                this.actionList.push(actions[i]);
            }
    }
	
	

	isComplex()    : boolean {return true}
	
	isExecutable() : boolean {return false}
	
	isEmpty()      : boolean {return this.actionList.length === 0}
}