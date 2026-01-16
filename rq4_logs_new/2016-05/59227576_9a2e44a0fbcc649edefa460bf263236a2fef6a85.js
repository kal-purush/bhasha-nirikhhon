var Dispatcher = require('../dispatcher/Dispatcher');
var AppConstants = require('../constants/AppConstants');

var UserActions = {
  addTransaction: function(item) {
    Dispatcher.handleViewAction({
      actionType: AppConstants.ADD_TRANSACTION,
      payload: item
    });
  },
};

module.exports = UserActions;