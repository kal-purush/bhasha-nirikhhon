

module Backbone{
	
	export class LocalStorageEnabledModel extends Model{
		changed: AnyArrayWithId;
		constructor(attributes?: any, options?: any){
			super(attributes, options);
		}
	}
}