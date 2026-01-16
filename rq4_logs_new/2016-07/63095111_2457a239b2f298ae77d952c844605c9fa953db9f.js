'use strict'

juke.directive('song', function () {
  return {
    restrict: 'E',
    templateUrl: '/js/song/songlist.html',
    scope: {
      songView: "="
    },
    link: function (scope) {
    }
  }
})