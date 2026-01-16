'use strict';

// Expose the service
angular.module('app.models').factory('fileModel', [function FileModel() {
    return {
        setFile: function(file) {
            this._file = file;
        },
        getFile: function(file) {
            return this._file;
        }
    };
}]);