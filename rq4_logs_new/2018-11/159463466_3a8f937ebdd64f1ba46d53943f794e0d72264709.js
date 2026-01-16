import { EventEmitter } from 'events';
EventEmitter.prototype._maxListeners = 25;
export default new EventEmitter();