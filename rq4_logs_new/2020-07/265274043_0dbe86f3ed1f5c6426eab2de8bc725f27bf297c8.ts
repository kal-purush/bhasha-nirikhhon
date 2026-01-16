
// usage: 
// var cm = new CallbackManager((query) => {
//   nanika.fetch(cm.getRecieveCallback());
// });
// // task1
// cm.post(myQuery, (result) => { });
// // task2 ...
// cm.post(myQuery, (result) => { });
export class CallbackManager {

  counter: number;
  sender: (any) => any;
  callbacks: { [index: number]: (data: any) => any };

  constructor(sender: (any) => any) {
    this.counter = 1;
    this.sender = sender;
    this.callbacks = {};
  }

  private generateCbmId(): number {
    return this.counter++;
  }

  post(data: any, callback: (data: any) => any): void {
    const cbmid = this.generateCbmId();
    // data = Object.assign({}, data, { id: { cbmid } }); // fail!
    data = { ...data };
    data.id.cbmid = cbmid;
    this.callbacks[cbmid] = callback;
    this.sender.call(null, data);
  }

  // continu: もう一度このcallbackを呼ぶ場合はtrue
  private handleRecieve(data: any, continu = false): void {
    if (!data) {
      console.warn('CallbackManager::handleRecieve: null recieved. reject.');
      return;
    }
    if (!data.id) {
      console.warn('CallbackManager::handleRecieve: id not found. reject.');
      return;
    }
    const cbmid = data.id.cbmid;
    if (!cbmid) {
      console.warn('CallbackManager::handleRecieve: id.cbmid not found. reject.');
      return;
    }
    data.id.cbmid = undefined;
    const cb = this.callbacks[cbmid];
    if (!cb) {
      console.warn('CallbackManager::handleRecieve: unknown id.cbmid. reject.');
      return;
    }
    if (!continu) {
      delete this.callbacks[cbmid];
    }
    cb.call(null, data);
  }

  getRecieveCallback() {
    return this.handleRecieve.bind(this);
  }
}

export default CallbackManager;