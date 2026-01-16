/// <reference path="../../ts-definitions/DefinitelyTyped/angularjs/angular.d.ts"/>

declare var TinySegmenter: any;

angular.module('egrid')
  .factory('separateText', ['$translate', ($translate) => {
    return (text: string) : string[] => {
      if ($translate.preferredLanguage() === 'ja') {
        return splitJapanese(text);
      } else {
        return text.split(' ')
      }
    };

    function splitJapanese(text: string) : string[] {
      var segmenter = new TinySegmenter();
      return segmenter.segment(text);
    }
  }]);

