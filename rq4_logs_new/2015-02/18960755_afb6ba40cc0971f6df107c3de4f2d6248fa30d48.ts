/// <reference path="../../ts-definitions/DefinitelyTyped/angularjs/angular.d.ts"/>

declare var kuromoji: any;

angular.module('egrid')
  .factory('getKuromojiTokenizer', ['$q', ($q) => {
    return () => {
      var deferred = $q.defer();
      kuromoji
        .builder({
          dicPath: "/dict/"
        })
        .build((err, tokenizer) => {
          deferred.resolve(tokenizer);
        });
      return deferred.promise;
    };
  }]);