/// <reference path="IEventEmitter.d.ts" />
/// <reference path="FAction.d.ts" />

interface INotifyPropertyChanged extends IEventEmitter {
	on(event: string, callback: Function); // IEventEmitter
	
	/** When a property has changed. */
	on(event: 'propertyChanged', callback: (propertyName: string) => void);
	
	/**
	 * Before a property might change.
	 * cancel is provided in the callback if cancellation is supported.
	 */
	on(event: 'propertyChanging', callback: (propertyName: string, cancel?: FAction) => void);
}