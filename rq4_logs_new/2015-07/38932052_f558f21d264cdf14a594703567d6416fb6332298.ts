interface IPayload {
  id:string;
  text:string;
  description:string;
}

interface IListener {
  event:string;
  func(payload?:IPayload);
}

class Dispatcher {
  private listeners:Array<IListener>;

  constructor() {
    this.listeners = [];
  }

  public registerListener(listener:IListener) {
    this.listeners.push(listener);
  }

  public publishEvent(type:string, data?:IPayload) {
    for (var i = 0, j = this.listeners.length; i < j; i += 1) {
      if (this.listeners[i]['event'] === type) {
        this.listeners[i]['func'](data);
      }
    }
  }
}