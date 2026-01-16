'use strict';


angular.module('myApp')
  .controller('AutoSuggestCtrl', function ($scope,$q) {
    var id;
    this.updateViewValue='';
    this.$index=-1;
    var self=this;
    this.setAutoSuggestOptions= function (option) {
      this.autoSuggestOptions=option;
    };

    this.setUpdateViewValueCallBack= function (callback) {
        this.updateViewValueCallback=callback;
    };

    this.trigger = function (UserInput) {
      var def = $q.defer();
      if (this.autoSuggestOptions.isRemote) {
        $q.when(this.autoSuggestOptions.load(UserInput)).then(function (data) {
          $scope['$autoSuggestSrc' + id] = data;
          def.resolve("success");
        });
      } else {
          $q.when(this.autoSuggestOptions.src).then(function (data) {
            $scope['$autoSuggestSrc' + id] = data;
            def.resolve("success");
          });
      }
      return def.promise;
    };
    this.setId = function (_id) {
      id = _id;
      $scope['$autoSuggestSrc' + id] = [];
      $scope['$onHover'+id] = function (event, index) {
        var el = angular.element(event.currentTarget);
        el.addClass('active');
        el.siblings().removeClass('active');
        self.$index = index;
      };
      $scope['$onSelect'+id] = function (item) {
        self.$index = -1;
        self.updateViewValueCallback(item[self.autoSuggestOptions.viewKey]);
        self.autoSuggestOptions.select(item);
      };
    };
    this.getId = function () {

      return id;
    };


  });