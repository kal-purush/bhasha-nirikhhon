import noop = require('../utils/noop');
import Operation = require('../operations/operation');

class SendingQueue {
  private _queue: Operation[] = [];

  context: number = 0;

  // called when an opration is ready to send
  onReadySend: (op: Operation) => void = noop;

  enqueue(op: Operation) {
    this._queue.push(op);

    if (this._queue.length === 1) {
      this.onReadySend(op);
    }
  }

  dequeue() : Operation {
    var op = this._queue.shift();

    if (this._queue.length > 0) {
      this.onReadySend(this._queue[0]);
    }

    return op;
  }

  updateOperation(op: Operation) {
    for (var i = 0, ii = this._queue.length; i < ii; i++) {
      if (this._queue[i].seqId === op.seqId) {
        this._queue.splice(i, 1, op);
      }
    }
  }
}

export = SendingQueue;