/// <reference path="./interfaces"/>
/// <reference path="./caller"/>
/// <reference path="./callback-container"/>

module App {
  import I = CallbackInfrastructure;

  /** Provides Callback */
  export class CallMe implements I.CanBeCalled {

    caller: I.CallOthers;
    callbackContainer: CallbackContainer;
    constructor() {
      this.callbackContainer = new CallbackContainer(this.cb);
      this.caller = new App.Caller(this.callbackContainer);
    }

    cb() {
      return 'I was called.';
    }
  }
}