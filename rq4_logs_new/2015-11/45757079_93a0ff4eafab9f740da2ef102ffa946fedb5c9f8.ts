/// <reference path="../../.tmp/typings/tsd.d.ts"/>

declare var componentHandler;
declare var moment: moment.MomentStatic;

declare module app {
	
	interface IProduct {
		id?: number;
		created: string;
		title: string;
		content: string;
		image: string;
		dollarPrice: string;
	}
	
	interface IProductService {
		addProduct (product: IProduct): ng.IHttpPromise<IProduct>;
		listProducts (): ng.IHttpPromise<[IProduct]>;
		getProduct (id: number): ng.IHttpPromise<IProduct>;
		updateProduct (id: number, product: IProduct): ng.IHttpPromise<IProduct>;
		removeProduct (id: number): ng.IHttpPromise<any>;
	}
	
}