app.controller('userController', function PostController($scope, userFactory) {
    $scope.users = [];
    $scope.user = null;
    $scope.editMode = false;
    $scope.Member;
    $scope.userAuthentication = "Fail";
    $scope.GeneralMessage = "";
    $scope.showAlert = false;
    $scope.Result;

    $('#divMessage').hide();
    $('#divOperations').hide();

    // initialize your users data
    $scope.login = function (Member) {


        if ($('#txtuserName').val() == null) {

            $scope.GeneralMessage = 'Username is reqiured,';
            $scope.showAlert = true;
        }
        if ($('#txtPassword').val() == '') {

            $scope.GeneralMessage = 'Password is reqiured, ';
            $scope.showAlert = true;
        }

        if ($('#txtPassword').val() != '' && $('#txtuserName').val() != '') {

            userFactory.login({ username: Member.userName, password: Member.Password }).success(function (data) {

                $scope.userAuthentication = 'Pass';
                $scope.showAlert = false;
                $('#divLogin').hide();
                $('#divOperations').show();

            }).error(function () {

                $scope.userAuthentication = 'Fail';
                $scope.GeneralMessage = "Please Check Your Username & Password";
                $scope.showAlert = true;
                $('#divMessage').show();
                $('#divLogin').show();
                $('#divOperations').hide();

            });

        }
    }



    // --------------- REGISTER NEW MEMEBER  -------------------
    $scope.RegisterMember = function (NewMember) {



        userFactory.registerNewMember(NewMember).success(function (data) {

            $scope.GeneralMessage = "User Created Please Login";

            // $('#divMessage').show();
            $('#myModal').modal('toggle');
        }).error(function () {
        });

    }


    //--------------- Sum ------------------------------
    $scope.Sum = function (Operation) {


        userFactory.Sum({ number1: Operation.number1, number2: Operation.number2 }).success(function (data) {

            $scope.Result = data;
            $('#txtSumResult').value = data;
        }).error(function () {

            $scope.userAuthentication = 'Fail';

        });

    }

    //--------------- Sub ------------------------------
    $scope.Sub = function (Operation) {

        userFactory.Sub({ number1: Operation.Subnumber1, number2: Operation.Subnumber2 }).success(function (data) {

            $scope.ResultSub = data;
        }).error(function () {

            console.log('Unexpected Error');

        });


    }
    //--------------- Multiply ------------------------------
    $scope.Multiply = function (Operation) {

        userFactory.Multiply({ number1: Operation.MultiplyNumber1, number2: Operation.MultiplyNumber2 }).success(function (data) {

            $scope.MultiplyResult = data;

        }).error(function () {

            console.log('Unexpected Error');


        });

    }
    //--------------- Divide ------------------------------
    $scope.Divide = function (Operation) {


        userFactory.Divide({ number1: Operation.DivideNumber1, number2: Operation.DivideNumber2 }).success(function (data) {

            $scope.DivideResult = data;

        }).error(function () {

            console.log('Unexpected Error');

        });

    }

});