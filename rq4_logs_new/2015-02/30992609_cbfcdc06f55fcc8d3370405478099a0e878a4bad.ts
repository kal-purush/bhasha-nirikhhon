/// <reference path="../../_references.d.ts" />

import plat = require('platypus');

import BaseViewControl = require('../base/base.viewcontrol');

class LoginViewControl extends BaseViewControl {
    /**
     * This is the property that indicates where the template HTML for this control exists.
     */
    templateString: string = require('./login.viewcontrol.html');

    /**
     * The context variable on a control corresponds to what can be used 
     * for data binding in the view.
     */
    context: any = { };

    /**
     * This is the initialize event method for a control. In this method a control 
     * should initialize all the necessary variables. This method is typically only 
     * necessary for view controls. If a control does not implement plat.ui.IViewControl 
     * then it is not safe to access, observe, or modify the context property in this 
     * method. A view control should call services/set context in this method in order 
     * to fire the loaded event. No control will be loaded until the view control has 
     * specified a context.
     */
    initialize() { }

    /**
     * This event is fired after all of the child controls of this control have loaded.
     * Since this is a view control, setting its context kicks off the binding and loading 
     * phases.
     */
    loaded() { }

    /**
     * This event is fired when a navigation state has been matched for this control. 
     * In this method you can return a boolean or Promise of a boolean indicating whether 
     * or not it is possible to navigate to this control in the current context of the app.
     * You must explicitly return false or a Promise of false to prevent navigation.
     */
    canNavigateTo(parameters: any, query: any) { }

    /**
     * This event is fired when a navigation state has been matched that is outside the context
     * for this control. In this method you can return a boolean or Promise of a boolean 
     * indicating whether or not it is possible to navigate from this control in the current 
     * context of the app. This can be useful for prompting the user to save state before navigating. 
     * You must explicitly return false or a Promise of false to prevent navigation.
     */
    canNavigateFrom() { }

    /**
     * This event is fired when this control is navigated to directly using the 
     * navigator.navigate method on a view control. The parameter corresponds to 
     * an object sent from the previous view control during navigation.
     */
    navigatedTo(parameters: any, query: any) { }

    /**
     * This event is fired when you are navigating away from this view control to another 
     * view control.
     */
    navigatingFrom() { }

    /**
     * The dispose event is called when a control is being removed from memory. A control 
     * should release all of the memory it is using, including DOM event and property 
     * listeners.
     */
    dispose() { }
}

plat.register.viewControl('login-vc', LoginViewControl);

export = LoginViewControl;