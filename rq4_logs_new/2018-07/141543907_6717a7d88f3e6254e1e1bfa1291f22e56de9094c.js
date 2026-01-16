app.controller('AdminController', ['$scope', 'AdminService', function ($scope, AdminService) {

    $scope.userCount = 0;
    $scope.movieCount = 0;
    $scope.saleCount = 0;
    $scope.categoryCount = 0;

    $scope.initialize = function () {
        $scope.countMovies();
        $scope.countUsers();
        $scope.countSales();
        $scope.countCategories();
    };

    $scope.countMovies = function () {
        AdminService.countMovies(function (response) {
            $scope.movieCount = response.data;
        }, function (response) {
            console.log("error in getting movie count");
        });
    };

    $scope.countUsers = function () {
        AdminService.countUsers(function (response) {
            $scope.userCount = response.data;
        }, function (response) {
            console.log("error in getting user count");
        });
    };

    $scope.countSales = function () {
        AdminService.countSales(function (response) {
            $scope.saleCount = response.data;
        }, function (response) {
            console.log("error occured while trying to get the sale count");
        });
    };

    $scope.countCategories = function () {
        AdminService.countCategories(function (response) {
            $scope.categoryCount = response.data;
        }, function (response) {
            console.log("error occurred while trying to get the category count");
        });
    };

    $scope.getAllCategories = function () {
        AdminService.getAllCategories(function (response) {
            $scope.categories = response.data;
        }, function (response) {
            console.log("an error occurred while trying to get the categories");
        });
    };

    $scope.deleteCategory = function (categoryId) {
        Pace.restart();
        AdminService.deleteCategory(categoryId, function (response) {
            $scope.getAllCategories();
        }, function (response) {
            console.log("an error occurred while trying to delete the category");
        });
    };

}]);

app.service('AdminService', ['APIService', function (APIService) {

    this.countUsers = function (successHandler, errorHandler) {
        APIService.get('/api/users/count', successHandler, errorHandler);
    };

    this.countMovies = function (successHandler, errorHandler) {
        APIService.get('/api/movies/count', successHandler, errorHandler);
    };

    this.countSales = function (successHandler, errorHandler) {
        APIService.get('/api/sales/count', successHandler, errorHandler);
    };

    this.countCategories = function (successHandler, errorHandler) {
        APIService.get('/api/categories/count', successHandler, errorHandler);
    };

    this.getAllCategories = function (successHandler, errorHandler) {
        APIService.get('/api/categories', successHandler, errorHandler);
    };

    this.deleteCategory = function (categoryId, successHandler, errorHandler) {
        APIService.delete('/api/category/delete/' + categoryId, successHandler, errorHandler);
    };
}]);