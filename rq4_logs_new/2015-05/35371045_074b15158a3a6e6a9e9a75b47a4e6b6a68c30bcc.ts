import Operation = require('./operation');

class OperationBuffer {
  private _buffer: Operation[] = [];

  /**
   * append the operation with transformations against context.
   * context: the count of operations that were executed before the target operation.
   */
  append(op: Operation, context: number) {
    if (this._buffer.length < context) {
      throw new Error('Invalid context: the context size is larger than the buffer size');
    }

    var transformed = op;
    for (var i = context, ii = this._buffer.length; i < ii; i++) {
      transformed = transformed.transformWith(this._buffer[i]);
    }

    this._buffer.push(transformed);
  }
}

export = OperationBuffer;