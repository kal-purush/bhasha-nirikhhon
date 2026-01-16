app.controller("RegisterCntrl", function($scope){
  $scope.roles = ["Top", "Jungle", "Mid", "ADC", "Support"];
  $scope.regions = ["NA", "EU"];
  $scope.newUser = {};

  $scope.newUser.roles = [];
  $scope.checkRole = (role)=>{
    let index = $scope.newUser.roles.indexOf(role);
    if(index!==-1){
      $scope.newUser.roles.splice(index,1);
    }else{
      $scope.newUser.roles.push(role);
    }
  }

  $scope.passwordConfirmation = false;

});