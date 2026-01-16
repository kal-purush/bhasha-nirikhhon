module.exports = {
    activityStart: function (labelText, options, successCallback, failureCallback) {
        successCallback();
    },
    activityStop: function(successCallback, failureCallback) {
        successCallback();
    }
};

require("cordova/exec/proxy").add("Spinnerplugin", module.exports);