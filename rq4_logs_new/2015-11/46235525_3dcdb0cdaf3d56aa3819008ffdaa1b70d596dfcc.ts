namespace functions1sample {
  "use strict";

  function add(a: number, b: number): number {
    return a + b;
  }

  interface Message {
    short: string;
    full: string;
  }

  function logIt(msg: Message, verbose: boolean) {
    console.log(msg.short);
    if (verbose) {
      console.log(msg.full);
    }
  }

  function logItDefault(msg: Message, verbose = false) {
    console.log(msg.short);
    if (verbose) {
      console.log(msg.full);
    }
  }

  // logit("not working", false); // error
}

namespace functions2sample {
  "use strict";

  interface MyEvent {
    type: string; value: any;
  }

  type MyListenerType = (event: MyEvent) => void;

  let listeners: MyListenerType[] = [];

  function addEventListener(listener: MyListenerType): void {
    listeners.push(listener);
  }

  interface MyListenerIf {
    (event: MyEvent): void;
  }

  let handler: MyListenerIf =
    (event) => console.log(event.type);

  addEventListener(handler);
}