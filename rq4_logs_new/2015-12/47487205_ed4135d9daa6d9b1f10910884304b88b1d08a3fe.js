'use strict';
import AppConstants from '../constants/app-constants';
import AppDispatcher from '../dispatcher/app-dispatcher';

class AppActions {
	addItem(item) {
		AppDispatcher.handleAction({
			actionType: AppConstants.ADD_ITEM,
			data: item
		});
	}

	removeItem(index) {
		AppDispatcher.handleAction({
			actionType: AppConstants.REMOVE_ITEM,
			data: index
		});
	}
}

export default new AppActions();