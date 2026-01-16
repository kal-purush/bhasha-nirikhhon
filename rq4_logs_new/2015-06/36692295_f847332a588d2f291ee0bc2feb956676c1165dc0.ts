/// <reference path="immutable.d.ts" />
declare module 'capacitor' {
	import immutable = require('immutable');
	type scalar = number|string;
	module Implementation {
		interface Action {
			toString: () => string;
		}
		
		interface ActionInstance {
			valueOf() : any;
			getActionID() : number;
		}
		
		interface EventBrokerInterface {
			add(fn: Function) : void;
			add(fn: Function, context: any) : void;
			
			remove(fn: Function) : void;
			remove(fn: Function, context: any) : void;
			
			addImmediate(fn: Function) : void;
			addImmediate(fn: Function, context: any) : void;
			removeImmediate(fn: Function) : void;
			removeImmediate(fn: Function, context: any) : void;	
		}
		
		interface EventBroker extends EventBrokerInterface {
			(): void;
			
			dispatch(): void;
			dispatchImmediate(...args : any[]);
		}
		
		interface ActionManager {
			create(name: string): Action;
			list(): string[];
			exists(name: string): boolean;
		}
		
		interface ActionCreator {
			dispatch(action: Action, payload: any) : ActionInstance;
			createActionInstance(action: Action, payload : any) : ActionInstance;
			generateRequestID() : number;
		}
		
		interface StoreInterface {
			changed: EventBrokerInterface;
			get(key: string): immutable.Iterable<scalar, any>;
			getIn(keys: string[]): any;
		}
			
		interface EntityStoreInterface extends StoreInterface {
			getItem(key: scalar) : immutable.Iterable<any, any>;
		}
		
		interface ListStoreInterface extends StoreInterface {
			getItem(id : scalar) : immutable.Iterable<any, any>;
			getItems() : immutable.List<immutable.Iterable<any, any>>;
		}
		
		interface IndexedListStore extends StoreInterface {
			getItems(index: scalar) : immutable.List<immutable.Iterable<any, any>>;
			getItem(index: scalar, id: scalar) : immutable.Iterable<any, any>
		}
		
		interface Store {}
		
		interface Dispatcher {
			isDispatching(): boolean;
			register(store: Store): number;
			unregister(store: Store): void;
			dispatch(actionInstance: Action): void;
		}
	}
	/**
	 * Throws an error with errorMessage as the error message if invariant is false
	 */
	export function invariant(invariant: boolean, errorMessage?: string): void;
	
	export var actionManager: Implementation.ActionManager;
	
	export class Store implements Implementation.Store {
		/**
		 * Defines a handler for action, calling the callback
		 * (where this is bound to the store for you) when the action is dispatched.
		 * Use the waitFor callback to wait on other stores before processing
		 * the action.
		 */
		static action(action: Implementation.Action, callback: (payload: any, waitFor: (...stores: Implementation.StoreInterface[]) => void) => void): void;
		/**
		 * Sets up a one-to-one or many-to-one relationship on this store.
		 * The result is that when getItem is called, at 'key' the result of calling
		 * entityStore.getItem(item[key]) will be placed. 
		 */
		static hasOne(key: string, entityStore: Implementation.StoreInterface): void;
		/**
		 * Sets up a one-to-many or many-to-many relationship on this store.
		 * The result is that when getItem is called, at 'key' the result of calling
		 * entityStore.getItems(item[key]) will be placed. 
		 */
		static hasMany(key: string, listStore: Implementation.StoreInterface): void;

		/**
		 * Interface object used to interact with the store.
		 * A store should never be accessed outside the store,
		 * through anything but this interface.
		 */
		getInterface(): Implementation.StoreInterface;
		
		initialize(): void;
		
		get(key: string) : immutable.Iterable<any, any>;
		set(key: string, val: any) : void;
		/**
		 * Same as passing any other value to set. However passing
		 * an immutable.Iterable<any, any> will not recurse through
		 * the object and turn it into immutables.  
		 */
		set(key: string, val: immutable.Iterable<any, any>);
		
		
		getRawItem(key: scalar) : immutable.Iterable<any, any>;
		setRawItem(key: scalar, val: any) : void;
		setRawItem(key: scalar, val: immutable.Iterable<any, any>) : void;
		
		changed: Implementation.EventBroker;
	}
		
	export class EntityStore extends Store {
		getItem(key: scalar) : immutable.Iterable<any, any>;
		getRawItem(key: scalar) : immutable.Iterable<any, any>;
		getRawItems() : immutable.Map<scalar, immutable.Iterable<any, any>>;
		setItem(item: any) : void
		setItem(item: immutable.Iterable<any, any>) : void;
		
		dereferenceItem<T extends immutable.Iterable<any, any>>(item: T) : T;
		
		setItems(items: immutable.Map<scalar, immutable.Iterable<any, any>>) : void;
		
		removeItem(id: scalar) : void;
		
		getItemsWithIds(ids: scalar[]) : immutable.List<immutable.Iterable<any, any>>;
		getItemsWithIds(ids: immutable.List<scalar>) : immutable.List<immutable.Iterable<any, any>>;
		
		getRawItemsWithIds(ids: scalar[]) : immutable.List<immutable.Iterable<any, any>>;
		getRawItemsWithIds(ids: immutable.List<scalar>) : immutable.List<immutable.Iterable<any, any>>;
		
		getInterface(): Implementation.EntityStoreInterface;
	}
	
	export class ListStore extends Store {
		getIds(): immutable.List<scalar>;
		setIds(ids: scalar[]) : void;
		setIds(ids: immutable.List<scalar>) : void;
		
		getItems(): immutable.List<immutable.Iterable<any, any>>;
		
		add(id: scalar) : void;
		add(ids: scalar[]) : void;
		add(ids: immutable.List<scalar>) : void;
		
		remove(id: scalar) : void;
		
		reset(id: scalar) : void;
		reset(ids: scalar[]) : void;
		reset(ids: immutable.List<scalar>) : void;
		
		getInterface(): Implementation.ListStoreInterface;
	}
	
	export class IndexedListStore extends Store {
		add(index: scalar, id: scalar) : void;
		add(index: scalar, ids: scalar[]) : void;
		add(index: scalar, ids: immutable.List<scalar>) : void;
		
		getIds(index: scalar) : immutable.List<scalar>;
		setIds(index: scalar, ids: scalar[]) : void;
		setIds(index: scalar, ids: immutable.List<scalar>) : void;
		
		remove(index: scalar, id: scalar) : void;
		
		removeIndex(index: scalar) : void;
				
		resetAll() : void;
		
		reset(index: scalar, id: scalar) : void;
		reset(index: scalar, ids: scalar[]) : void;
		reset(index: scalar, ids: immutable.List<scalar>) : void;
		
		getItems(index: scalar) : immutable.List<immutable.Iterable<any, any>>;
		getItem(index: scalar, id: scalar) : immutable.Iterable<any, any>
	}
		
	export var Dispatcher: Implementation.Dispatcher;
}