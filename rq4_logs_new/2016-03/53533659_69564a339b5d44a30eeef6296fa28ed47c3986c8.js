angular.module("codeLibrary").controller('gitCtrl', function($scope, gitService) {

    $(document).ready(function() {
        $('.collapsible').collapsible({
            accordion: false // A setting that changes the collapsible behavior to expandable instead of the default accordion style
        });
    });

});