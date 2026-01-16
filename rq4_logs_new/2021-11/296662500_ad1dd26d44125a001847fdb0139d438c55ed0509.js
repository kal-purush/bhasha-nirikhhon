angular.module('newApp').controller('earningsCtrl', function($scope, $http, $filter, $timeout) {

    $scope.currentPage = 1;
    $scope.pageSize = 10;
    $scope.pagedata = [];

    $scope.pageChangeHandler = function(num) {
        console.log('pagedata page changed to ' + num);
    };

    $scope.pageChangeHandler = function(num) {
        console.log('going to page ' + num);
    };

    firebase.database().ref('/earnings/').orderByChild('mobileno').on("value", function(snapshot) {

        $timeout(function() {
            $scope.$apply(function() {
                let returnArr = [];
                snapshot.forEach(childSnapshot => {
                    let item = childSnapshot.val();
                    item.key = childSnapshot.key;

                    var data = {
                        email: item.email,
                        key: item.key,
                        mobileno: item.contact,
                        quotes: item.quotes,
                        total: item.amount,
                        datepaid: item.datepaid,
                        mode: item.mode,
                        amtpaid: parseFloat(item.amtpaid),
                        balance: item.balance
                    }

                    returnArr.push(data);

                    // returnArr.push(item);
                });

                $scope.earnings = returnArr;
                console.log(returnArr);

            });

        })

    });







})