function Action(target: any) {
    var original = target;
    var className = target.name;
    var actions: string[] = [];
    var methods = Object.getOwnPropertyNames(target.prototype);
    console.log(methods);
    for (var i = 0; i < methods.length; i++) {
        let methodName = methods[i];
        let origMethod = target.prototype[methodName];
        target.prototype[methodName] = function() {
            dispatch(className + '.' + methodName, arguments);
            return origMethod.apply(this, arguments);
        }
        target.prototype[methodName].displayName = className + '.' + methodName;
    }
}

var dataListeners: { [type: string]: (data: any) => void } = {};
function dispatch(type: string, data: any) {
    console.log("dispatch", type, data);
    if (dataListeners[type]) {
        dataListeners[type](data);
    }
}
function onData<T>(type: string, callback: (data: T) => void) {
    dataListeners[type] = callback;
}

function dispatchAsync<T>(dtype: Function | string, callback: (data: T) => void) {
    var type = typeof dtype == 'function' ? (<any>dtype).displayName + '.async' : dtype;
    return (data: T) => {
        onData(type, callback);
        dispatch(type, data);
    }
}

declare var fetch: <T>() => Promise<T>;
class Socket {
    constructor(url: string, callback: (data: number) => void) {
        setInterval(() => callback(Math.random()), 1000);
    }
}

class REST {
    get() {
        return new Promise((resolve) => setTimeout(() => resolve(Math.random()), 1000));
    }
    put(id: number, params: any) {
        return new Promise((resolve) => setTimeout(() => resolve(Math.random()), 1000));
    }
    post() {

    }
    delete() {

    }
}
class PostRest extends REST {

}

var postRest = new PostRest();
@Action
class PostActions {
    fetch(id: number) {
        return new Promise((resolve) => {
            postRest.get().then(dispatchAsync(this.fetch, data => {
                resolve(data);
            }));
        });
    }

    subscribe() {
        new Socket('abc', dispatchAsync(this.subscribe, data => {

        }));
    }

    update(id: number, params: { title: string }) {
        return new Promise((resolve) => {
            postRest.put(id, params).then(dispatchAsync(this.update, data => {
                resolve();
            }))
        });
    }

    createSync(id: number, title: string) {

    }
}
var actions = {
    post: new PostActions()
}

actions.post.fetch(123).then(data => console.log(data))

// actions.post.subscribe()

actions.post.createSync(1, "#$")