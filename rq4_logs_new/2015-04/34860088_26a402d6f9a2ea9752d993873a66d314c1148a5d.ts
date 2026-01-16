module Komponents.Utils {
	
	export function keys(value: any) {
		let keys = <string[]>[];
		for (var key in value) {
			if (Object.prototype.hasOwnProperty.call(value, key)) {
				keys.push(key);
			}
		}
		return keys;
	} 
	
	export function toEnum<T>(value: string | KnockoutObservable<string>, type: any) {
		let result: T = null;
		let ks = keys(type);
		let pattern = new RegExp(`^${value}$`, "i");
		
		for (let key of ks) {
			if (pattern.test(key)) {
				result = type[key];
				break;
			}
		}
		
		return result;
	}
}