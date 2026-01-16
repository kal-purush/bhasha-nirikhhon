import {EventEmitter} from 'events';
import utils from '../utils/utils';

export default class BaseStore extends EventEmitter {
	CHANGE_EVENT: string = 'change';
	utils = utils;

	addChangeListener(cb) {
		this.on(this.CHANGE_EVENT, cb);
	}

	removeChangeListener(cb) {
		this.removeListener(this.CHANGE_EVENT, cb);
	}

	emitChange() {
		this.emit(this.CHANGE_EVENT);
	}
}
