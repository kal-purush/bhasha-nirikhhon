interface Property {
    default?:any;
	plain?:string;
	type?:string;
	dtype?:string|FunctionConstructor;
	inverse?:string;
	scenario?:string;
	formatter?:Function|string;
	parser?:Function|string;
	getValue?:Function;
	setValue?:Function;
}