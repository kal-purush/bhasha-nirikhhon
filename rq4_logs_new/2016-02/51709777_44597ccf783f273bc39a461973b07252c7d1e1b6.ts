/// <reference path="typings/main.d.ts" />

import {History, Location, LocationDescriptor} from "history";
import {IMiddleware, IStore, IAction, IReducer} from "redux";

declare module reactRouterRedux {
	export interface IRouterReduxMiddleware<TState> extends IMiddleware<TState> {
		/**
		 * By default, the syncing logic will not respond to replaying of actions, which means it won't work with projects like redux-devtools. Call this function on the middleware object returned from syncHistory and give it the store to listen to, and it will properly work with action replays.
		 * Obviously, you would do that after you have created the store and everything else has been set up.
		 * Supply an optional function selectLocationState to customize where to find the location state on your app state. It defaults to state => state.routing.location, so you would install the reducer under the name "routing". Feel free to change this to whatever you like.
		 * 
		 * @param {IStore<TState>} store (description)
		 * @param {(state:TState) => Location} selectLocationState (description)
		 */
		listenForReplays(store: IStore<TState>, selectLocationState?: (state: TState) => Location);

		/**
		 * Call this on the middleware returned from syncHistory to stop the syncing process set up by listenForReplays.
		 */
		unsubscribe();
	}

	/**
	 * Call this to create a middleware that can be applied with Redux's applyMiddleware to allow actions to call history methods. The middleware will look for route actions created by push, replace, etc. and applies them to the history.
	 * 
	 * @export
	 * @template TState
	 * @param {History} history (description)
	 * @returns {IRouterReduxMiddleware<TState>} (description)
	 */
	export function syncHistory<TState>(history: History): IRouterReduxMiddleware<TState>;

	/**
	 * A reducer function that keeps track of the router state. You must add this reducer to your app reducers when creating the store. 
	 * It will return a location property in state. If you use combineReducers, it will be nested under wherever property you add it to (state.routing in the example above).
	 * Warning: It is a bad pattern to use react-redux's connect decorator to map the state from this reducer to props on your Route components. This can lead to infinite loops and performance problems. react-router already provides this for you via this.props.location.
	 * 
	 * @export
	 * @param {Location} state (description)
	 * @param {IAction} action (description)
	 * @returns {Location} (description)
	 */
	export function routeReducer(state: Location, action: IAction): Location;

	export interface IRouteActions {
		push(nextLocation: LocationDescriptor): IAction;
		replace(nextLocation: LocationDescriptor): IAction;
		go(n: number): IAction;
		goForward(): IAction;
		goBack(): IAction;
	}

	/**
	 * An object that contains all the actions creators you can use to manipulate history:
	 * 
	 * @export
	 */
	export const routeActions: IRouteActions;

	/**
	 * Action type for when the location is updated
	 * @export
	 */
	export const UPDATE_LOCATION: string;

	export const TRANSITION: string;
}

export = reactRouterRedux;