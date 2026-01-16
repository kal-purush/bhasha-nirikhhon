namespace et {
	export interface ExistConditionData extends ConditionData {
		locator:LocatorData[];
	}
	
	/**
	 * ExistCondition
	 */
	export class ExistCondition extends Condition {
		constructor() {
			super();
		}
		
		locators:Locator[];
		
		parse(data:ExistConditionData,stage:egret.Stage) {
			super.parse(data,stage);
			if(!data)
				return;
			if(data.locator&&data.locator.length) {
				this.locators = data.locator.map(l=>Locator.parse(l,stage));
			}
		}
		
		execute():boolean{
			var result = super.execute();
			if(!result)
				return false;
			
			if(!this.locators||this.locators.length==0)
				return false;
				
			for (var index = 0; index < this.locators.length; index++) {
				var locator = this.locators[index];
				var found = locator.execute();
				if(found)
					return true;
			}
			
			return false;
		}
	}
}