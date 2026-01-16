/// <reference path='../_references.d.ts' />

import plat = require('platypus');
import HomeViewControl = require('../viewcontrols/home/home.viewcontrol');

export class App extends plat.App {
    /**
     * Class for every app. This class contains hooks for Application Lifecycle Events
     * as well as error handling and device-events.
     */
    constructor(router: plat.routing.Router) {
        super();
        
        router.configure([
            { pattern: '', view: HomeViewControl }
        ]);
    }

    /**
     * Event fired when the app is ready. This method can be used to 
     * configure various settings prior to loading the rest of the application
     * 
     * @param ev The LifecycleEvent object.
     */
    ready(ev: plat.events.LifecycleEvent) { }

    /**
     * Event fired when an internal error occurs.
     */
    error(ev: plat.events.ErrorEvent<Error>) {
        // log or handle errors at a global level
        console.log(ev.error);
    }

    /**
     * Event fired when the app is suspended. If running on a device, 
     * this is where you want to save important data and finish ongoing processes.
     */
    suspend(ev: plat.events.LifecycleEvent) { }
    
    /**
     * Event fired when the app has been programatically shutdown. This event is cancelable.
     */
    exiting(ev: plat.events.LifecycleEvent): void { }

    /**
     * Event fired when the app resumes from the suspended state. If running on a device,
     * this is where you want to re-initialize the app state. This is called only when the app was 
     * previously suspended.
     */
    resume(ev: plat.events.LifecycleEvent) { }

    /**
     * Event fired when the device regains connectivity and is now in an online state.
     */
    online(ev: plat.events.LifecycleEvent) { }

    /**
     * Event fired when the device loses connectivity and is now in an offline state.
     * 
     * @param ev The ILifecycleEvent object.
     */
    offline(ev: plat.events.LifecycleEvent) { }
}

plat.register.app('Getting-started', App, [
    plat.routing.Router
]);