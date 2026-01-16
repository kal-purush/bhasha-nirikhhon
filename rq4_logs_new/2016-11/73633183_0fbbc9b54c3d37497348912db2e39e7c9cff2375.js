/// <reference path="../angular.js" />

var MyApp = angular.module("MyApp", ['ngRoute', 'BooksService']);

MyApp.config(['$routeProvider',
    function ($routeProvider) {
        $routeProvider.
        when('/Add', {
            templateUrl: '/Home/Add',
            controller: 'AddController'
        }).
        when('/Edit', {
            templateUrl: '/Home/Edit',
            controller: 'EditController'
        }).
        when('/Delete', {
            templateUrl: '/Home/Delete',
            controller: 'DeleteController'
        }).
        when('/Books', {
            templateUrl: 'Home/Books',
            controller: 'HomeController'
        }).
        otherwise({
            redirectTo: '/Books'
        });
    }]);


MyApp.controller("AddController", function ($scope, BooksApi) {

    $scope.addBook = function () {
        var bookModel = {
            'Id': $scope.id,
            'Name': $scope.name,
            'Age': $scope.age,
            'Book_Type': $scope.book_type,
            'Author_Name': $scope.author_Name,
            'Publication': $scope.publication,
        };


        BooksApi.CreateBook(bookModel)
            .success(function (response) {
                alert('book added');

                $scope.name = undefined,
                $scope.age = undefined,
                $scope.book_type = undefined,
                $scope.author_Name = undefined,
                $scope.publication = undefined;
            }).
            error(function (response) {
              
                alert('Current book type does not exist!!!');
            });
    };
   

});
MyApp.controller("EditController", function ($scope) {

    $scope.message = "in Edit View";
});
MyApp.controller("DeleteController", function ($scope) {

    $scope.message = "in Delete View";
});


MyApp.controller("HomeController", function ($scope, BooksApi) {
 
    BooksFullInfo();
    GetGenres();
    $scope.FindByTypeId = function ($event)
    {

        OnGenreChange($event.currentTarget.dataset['bookTypeId']);
    };

    function OnGenreChange(bookTypeId)
    {
        BooksApi.BooksFullInfoByTypeId(bookTypeId).success(function (booksByTypeId) {
            $scope.books = booksByTypeId;
        })
       .error(function (error) {
           $scope.status = 'Unable to load booksByTypeId data' + error.message;
       })

    };


    function GetGenres()
    {
        BooksApi.GetGenres().success(function (genres)
        {
            $scope.genres = genres;
        })
        .error(function (error) {
            $scope.status = 'Unable to load genre data' + error.message;
        })
    };


    function BooksFullInfo() {       

        BooksApi.BooksFullInfo().success(function (books) {
            $scope.books = books;
        })
            .error(function (error) {
                $scope.status = 'Unable to load books data' + error.message;
            })
    };
});