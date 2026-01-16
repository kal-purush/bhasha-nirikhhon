import {PropertyChangedEventArgs} from "./events/property-changed-event-args";

/**
 * Defines an interface for objects that represent a view that can be bound.
 */
export interface IView {
    
    /**
     * Gets the helper that should be used to watch for changes on the view.
     */
    __viewBindingHelper: IViewBindingHelper;
}

/**
 * Defines an interface that represents a view binding helper. That is, an object that 
 * provides watch/observe capabilities on an object. 
 */
export interface IViewBindingHelper {
    /**
     * Sets the given property on the given object.
     * @param obj The object whose property should be set.
     * @param property The name of the property that should be set. Always only one property, and not a property list.
     * @param value The value that should be set.
     */
    setProp(obj: Object, property: string, value: any): void;
    
    /**
     * Watches the given property on the given object and calls the given callback function when a change occurs.
     * @param obj The object whose property should be observed.
     * @param property The name of the property that should be watched. Always only one property, and not a property list. (i.e. "myProp" and not "myProp.myValue")
     * @param callback The function that should be called when the property's value has changed.
     * @return A function that, when called, disposes of the observer.
     */
    observeProp(obj: Object, property: string, callback: (args: PropertyChangedEventArgs<any>) => void): Function;
}