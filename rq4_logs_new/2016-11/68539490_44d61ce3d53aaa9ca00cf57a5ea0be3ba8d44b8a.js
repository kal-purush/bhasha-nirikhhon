// app.js
(function(){
    angular
        .module("TemplateUrlBindData", ["ngRoute", "jgaTable"])
        .controller("tableController", function($scope){

            // declare user array containing user object instances
            var my_users = [
                {first: "Alice", last: "Wonderland", email: "alice@email.com"},
                {first: "Bob", last: "Hope", email: "bob@oscars.com"},
                {first: "Charlie", last: "Brown", email: "charlie@schultz.com"}
            ];

            // bind user array to view
            // view can refer to array using 'users' variable
            $scope.users = my_users;
        });
})();