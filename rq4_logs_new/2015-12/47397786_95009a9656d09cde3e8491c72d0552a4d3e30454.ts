"use strict";

export interface TypeInfoStatic {
	/** returns a TypeInfo from a test subject */
	<T>(subject: T): TypeInfo<T>;

	/** returns an array of TypeInfo from the current function arguments */
	fromArgs(args: IArguments): TypeInfo<any>[];
}

export interface TypeInfo<T> {
	/** current subject */
	subject: T;

	/** check if the subject is null (but not undefined) */
	isNull: boolean;

	/** check if the subject is undefined (but not null) */
	isUndefined: boolean;

	/** check if the subject is null or undefined */
	isNullOrUndefined: boolean;

	/** check if the subject is a primitive value (i.e. number, boolean, string, symbol) */
	isPrimitive: boolean;

	/** check if the subject is a primitive boolean value (but not a Boolean object) */
	isPrimitiveBoolean: boolean;

	/** check if the subject is a Boolean object (but not a primitive) */
	isBooleanObject: boolean;

	/** check if the subject is either a Boolean object, or a boolean primitive */
	isBoolean: boolean;

	/** check if the subject is a primitive number value (but not a Number object) */
	isPrimitiveNumber: boolean;

	/** check if the subject is a Number object (but not a primitive) */
	isNumberObject: boolean;

	/** check if the subject is either a Number object, or a number primitive */
	isNumber: boolean;

	/** check if the subject is a primitive string value (but not a String object) */
	isPrimitiveString: boolean;

	/** check if the subject is a String object (but not a primitive) */
	isStringObject: boolean;

	/** check if the subject is either a String object, or a string primitive */
	isString: boolean;

	/** check if the subject is a symbol (i.e. a primitive symbol) */
	isSymbol: boolean;

	/** check if the subject is an object */
	isObject: boolean;

	/** check if the subject is a function */
	isFunction: boolean;

	/** check if the subject is an array */
	isArray: boolean;

	/** check if the subject is an array in which the type of every element satisfies a predicate */
	isArrayOf(predicate: (ti: TypeInfo<any>) => boolean): boolean;

	/** check if the subject is an iterable (i.e. can be used in a for...of look) */
	isIterable: boolean;

	/** chekc if the subject is an object of a certain type
	 * @param type the constructor function of the type the subject is checked against
	 */
	isObjectOfType(type: Function): boolean;

	/** map current TypeInfo through a selector function
	 * @param selector the selector function
	 */
	map<U>(selector: (t: T) => U): TypeInfo<U>;

	/** map current TypeInfo through a selector function which also returns a TypeInfo
	 * @param selector the selector function
	 */
	flatMap<U>(selector: (t: T) => TypeInfo<U>): TypeInfo<U>;
}

class TypeInfoImpl<T> implements TypeInfo<T> {
	subject: T;

	constructor (target: any) {
		this.subject = target;
	}

	get isNull(): boolean {
		return this.subject === null && (typeof this.subject) === "object";
	}

	get isUndefined(): boolean {
		return this.subject == null && (typeof this.subject) === "undefined";
	}

	get isNullOrUndefined(): boolean {
		return this.subject == null;
	}

	get isPrimitive(): boolean {
		return (
			this.isNullOrUndefined ||
			this.isPrimitiveBoolean ||
			this.isPrimitiveString ||
			this.isPrimitiveNumber ||
			this.isSymbol
		);
	}

	get isPrimitiveBoolean(): boolean {
		return (typeof this.subject) === "boolean";
	}

	get isBooleanObject(): boolean {
		return this.isObjectOfType(Boolean);
	}

	get isBoolean(): boolean {
		return this.isPrimitiveBoolean || this.isBooleanObject;
	}

	get isPrimitiveNumber(): boolean {
		return (typeof this.subject) === "number";
	}

	get isNumberObject(): boolean {
		return this.isObjectOfType(Number);
	}

	get isNumber(): boolean {
		return this.isPrimitiveNumber || this.isNumberObject;
	}

	get isPrimitiveString(): boolean {
		return (typeof this.subject) === "string";
	}

	get isStringObject(): boolean {
		return this.isObjectOfType(Boolean);
	}

	get isString(): boolean {
		return this.isPrimitiveString || this.isStringObject;
	}

	get isSymbol(): boolean {
		return (typeof this.subject) === "symbol";
	}

	get isObject(): boolean {
		return (typeof this.subject) === "object" && this.subject !== null;
	}

	get isFunction(): boolean {
		return (typeof this.subject) === "function";
	}

	get isArray(): boolean {
		return (
			(typeof this.subject) === "object" &&
			this.subject !== null &&
			this.subject instanceof Array
		);
	}

	isArrayOf(predicate: (ti: TypeInfo<any>) => boolean): boolean {
		if (this.isArray) {
			const array = <any[]><any>(this.subject);
			for (const i of array) {
				const ti = new TypeInfoImpl<any>(i);
				if (!predicate(ti)) {
					return false;
				}
			}

			return true;
		}

		return false;
	}

	get isIterable(): boolean {
		return (!this.isNullOrUndefined) && (new TypeInfoImpl<any>(this.subject[Symbol.iterator])).isFunction;
	}

	isObjectOfType(type: Function): boolean {
		return (
			(typeof this.subject) === "object" &&
			this.subject !== null &&
			this.subject instanceof type
		);
	}

	map<U>(selector: (t: T) => U): TypeInfo<U> {
		return new TypeInfoImpl<U>(selector(this.subject));
	}

	flatMap<U>(selector: (t: T) => TypeInfo<U>): TypeInfo<U> {
		return selector(this.subject);
	}
}

const typeInfo: TypeInfoStatic = <TypeInfoStatic>function<T>(subject: T): TypeInfo<T> {
	return new TypeInfoImpl<T>(subject);
};

typeInfo.fromArgs = function (args: IArguments): TypeInfo<any>[] {
	const ti: TypeInfo<any>[] = [];
	for (let i = 0; i < arguments.length; i++) {
		args[i] = typeInfo(args[i]);
	}

	return ti;
};

export default typeInfo;