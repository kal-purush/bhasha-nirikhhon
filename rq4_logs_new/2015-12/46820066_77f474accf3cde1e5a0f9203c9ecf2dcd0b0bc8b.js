/**
 * @author rik
 */
import _ from 'lodash';
import events from 'events';

function Listener(model) {
  const props = {
    eventEmitter: {
      value: new events.EventEmitter()
    },
    model: {
      value: model
    },
    _internalListeners: {
      value: []
    },
    listeners: {
      value: []
    }
  };

  return Object.create(Listener.prototype, props);
}

Listener.prototype = {

  listenTo(model, event, callback) {
    const self = this;

    if (typeof model === 'function') {
      callback = model;
      model = null;
      event = null;
    } else if (typeof event === 'function') {
      callback = event;
      event = null;
    }

    const listenerObj = makeListenerObject.call(this, model, event, callback);
    self.listeners.push(listenerObj);

    bindInternalModelEventListener.call(this, event);

    return listenerObj;
  },

  trigger(event, data) {
    this.eventEmitter.emit(event, data);
  },

  on(event, callback) {
    bindInternalModelEventListener.call(this, event);

    this.eventEmitter.on(event, callback);
  },

  once(event, callback) {
    function cb(data) {
      this.off(event, cb);
      callback(data);
    }

    this.on(event, cb);
  },

  off(event, callback) {
    if (callback) {
      this.eventEmitter.removeListener(event, callback);
    } else {
      this.eventEmitter.removeAllListeners(event);
    }
  }

};

function makeListenerObject(model, event, callback) {
  const self = this;
  const listenerObj = {
    model,
    event,
    callback,
    stop() {
      const index = self.listeners.indexOf(listenerObj);

      if (index !== -1) {
        self.listeners.splice(index, 1);
      }
    }
  };

  return listenerObj;
}

function runModelListeners(event, model) {
  let callbackData = null;
  let filter = null;

  if (typeof model === 'object') {
    callbackData = model;
    filter = listener => {
      if (!listener.model) {
        return true;
      }

      const changedId = this.model.id(model);
      const listenerId = this.model.id(listener.model);
      const listenerEvent = listener.event;

      return (!listenerEvent || listenerEvent === event)
        && (model === listener.model || (changedId && changedId == listenerId));
    };
  } else {
    filter = listener => {
      const listenerEvent = listener.event;
      return !!(!listener.model && (!listenerEvent || event === listenerEvent));
    };
  }

  _.each(_.filter(this.listeners, filter), listener => {
    listener.callback(callbackData);
  });
}

function bindInternalModelEventListener(event) {
  if (this._internalListeners.indexOf(event) === -1) {
    this._internalListeners.push(event);

    this.on(event,  (model) => {
      runModelListeners.call(this, event, model);
    });
  }
}

export default Listener;