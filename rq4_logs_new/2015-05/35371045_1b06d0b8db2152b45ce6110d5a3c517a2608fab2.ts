import Operation = require('./operation');
import AddOperation = require('./add-operation');
import UpdateOperation = require('./update-operation');
import RemoveOperation = require('./remove-operation');

class OperationFactory {
  private _siteId: number;
  private _maxSeqId: number = 0;

  constructor(siteId: number) {
    this._siteId = siteId;
  }

  create(type: string, position: number, value: string) {
    var op;
    this._maxSeqId++;

    switch (type) {
      case 'add':
        op = new AddOperation(this._siteId, this._maxSeqId);
        break;
      case 'update':
        op = new UpdateOperation(this._siteId, this._maxSeqId);
        break;
      case 'remove':
        op = new RemoveOperation(this._siteId, this._maxSeqId);
        break;
      default:
        throw new Error('Invalid operation type: ' + type);
    }

    op.position = position;
    op.value = value;

    return op;
  }
}

export = OperationFactory;