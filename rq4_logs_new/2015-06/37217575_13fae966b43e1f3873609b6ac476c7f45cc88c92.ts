module Controllers{
    export class HomeController{
        message = "From Home controller";
        constructor($scope){
            $scope.vm = this;
        }
    }
}