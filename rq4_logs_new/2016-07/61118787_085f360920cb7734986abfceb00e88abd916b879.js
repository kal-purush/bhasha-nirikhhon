cibelApp.controller('navbarController', ['$scope', '$location', '$anchorScroll', function($scope, $location, $anchorScroll) {
  function restoreSectionsVisibility() {
    $("#main").hide();
    $("#home").show();
    $("#products").show();
    $("#advice").show();
    $("#inspiration").show();
    $("#company").show();
  }

  $scope.goToSection = function(sectionId) {
    restoreSectionsVisibility();
    $location.hash(sectionId);
    $anchorScroll();
  }
}]);