///<reference path="../Utils/Collections/List.ts"/>
///<reference path="../core/GameEvents.ts"/>

//module core {

	/*
	 *  IAction
	 */
	interface IAction {
		
		getNextAction(): IAction;
		
		isComplex(): boolean;
		
		isExecutable(): boolean;
		
		isEmpty(): boolean;
		
		executeAction(actionChain: Array<IAction>): void;
		
		parent: IAction;
	}
	
	

	/*
	 *  SimpleAction
	 */	
	class SimpleAction implements IAction {
		
		parent: IAction = null;
		
		runAction : (self: IAction, actionChain: Array<IAction>) => void = null;
		
		constructor (parent: IAction) {
			this.parent = parent;
		}
		
		getNextAction(): IAction {
			return null;
		}
		
		isComplex(): boolean {
			return false;
		}
		
		isExecutable(): boolean {
			return true;
		}
		
		isEmpty(): boolean {
			return this.runAction === null;	
		}
		
		executeAction(actionChain: Array<IAction>): void {
			this.runAction === null || this.runAction(this, actionChain);	
		};
		
	    toString(): string {
	        return this.constructor['name'];
	    }
	}




	
	/**
	 *  AtomicAction
	 */
	class AtomicAction extends SimpleAction {
		
		private optionalName: string = '';
		
		constructor (parent: IAction, 
		             runAction: (self: IAction, actionChain: Array<IAction>) => void, 
			         optionalName: string = '') {
			super(parent);
			this.runAction = runAction;
			this.optionalName = optionalName;
		}
		
	    toString(): string {
	        return (this.optionalName === '') ? this.constructor['name'] : this.optionalName;
	    }
	}
	



	/*
	 *  ComplexAction
	 */	
	class ActionList implements IAction {
		
		parent: IAction = null;
		
		actionsFIFO : Array<IAction> = [];
		
		constructor(parent: IAction) {
			this.parent = parent;
		}
		
		getNextAction(): IAction {
			return this.actionsFIFO.shift();
		}
		
		isComplex(): boolean {
			return true;
		}
		
		isExecutable(): boolean {
			return false;
		}
		
		isEmpty(): boolean {
			return this.actionsFIFO.length === 0;	
		}
		
		executeAction(actionChain: Array<IAction>): void {
			throw new Error('not executable'); 
		};		
		
	    toString(): string {
	        return this.constructor['name'];
	    }
	}
	

	interface IStackError2 { (stack: ActionStack2, action: IAction, actionChain: Array<IAction>, error: Error): void; }
	
	interface IStackAction2 { (stack: ActionStack2, action: IAction): void; }
	
	interface IStackOnAction2 { (stack: ActionStack2, action: IAction, actionChain: Array<IAction>): void; }
	
	interface IStackComplete2 { (stack: ActionStack2): void; }

	/*
	 *  ActionStack
	 */
	class ActionStack2 {
		
	    onError : Collections.List<IStackError2> = new Collections.List<IStackError2>();
	
	    onActionSuccess : Collections.List<IStackOnAction2> = new Collections.List<IStackOnAction2>();
	
	    onStackComplete : IStackComplete2 = null;
	
	    private internalOnStackComplete : IStackComplete2 = null;
	
	    onBeforeAction : Collections.List<IStackOnAction2> = new Collections.List<IStackOnAction2>();
	
	    onPutActionOnStack : IStackAction2 = null;
	    

		
		private stackFIFO : Array<IAction> = [];
		
	    /*
	     *  putOnStack
	     */  
	    putOnStack(action :IAction): void {
	        this.stackFIFO.push(action);
	        this.onPutActionOnStack === null || this.onPutActionOnStack(this, action);
	    }
	    
	    run(): void {
	    	if (this.stackFIFO.length != 0) {
	    		var action: IAction = this.stackFIFO[0];
				
				if (action.isEmpty()) {
					// akcja jest pusta - nie została usunięta, dziwne
					this.stackFIFO.shift();
					this.run();	
				} else {
					action.isComplex() && this.runComplexAction(action);
					!action.isComplex() && this.runAction(action);
				}
	    	} else 
	    		this.onStackComplete === null || this.onStackComplete(this);
	    }
	    
	    private runComplexAction(action: IAction): void {
			this.stackFIFO.unshift(action.getNextAction());
			this.run();	    	
	    }
	    
	    private runAction(action: IAction): void {
	    	this.stackFIFO.shift();
	    	if (action.isExecutable())
	    		try {
	    			var chain: Array<IAction> = this.getChain(action);
	    			
		            if (this.onBeforeAction != null)
		                for (var i = 0; i < this.onBeforeAction.length(); i++)
		                    this.onBeforeAction.get(i)(this, action, chain);
	    			
	    			action.executeAction(chain);
	    			
		            if (this.onActionSuccess != null)
		                for (var i = 0; i < this.onActionSuccess.length(); i++)
		                    this.onActionSuccess.get(i)(this, action, chain);
				} catch (err) {
		            if (this.onError != null)
		                for (var i = 0; i < this.onError.length(); i++)
		                    this.onError.get(i)(this, action, chain, err);
				}	    		
			this.run();
	    }
	    
	    private getChain(action: IAction): Array<IAction> {
	    	if (action.parent === null)
	    		return [action];
	    	else {
	    		var result : Array<IAction> = this.getChain(action.parent);
	    		result.push(action);
	    		return result;
	    	}
	    } 

	}
	 

//}