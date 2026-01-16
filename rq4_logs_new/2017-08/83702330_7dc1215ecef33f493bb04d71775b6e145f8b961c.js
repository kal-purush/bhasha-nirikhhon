app.controller('BalanceController', ['$scope', '$compile', '$window', function ($scope, $compile, $window) {

        //contacts arrays
        $scope.client = $window.client;
        $scope.workPoints = $window.workPoints;
        $scope.workPointsModified = [];
        $scope.workPointsIds = {};
        $scope.workPointsSums = {};
        $scope.currentIndex = 0;
        $scope.processing = false;
        $scope.success = false;
        $scope.error = false;

        //console.log($scope.workPoints);
        $scope.modifyParent = function (pid, index) {
            $scope.workPointsIds[pid][index] = 0;
            angular.forEach($scope.workPointsSums[pid], function (e, i) {
                $scope.workPointsIds[pid][index] += e[index] ? +e[index] : 0;
            });

            if ($scope.workPointsIds[pid].level > 1)
                $scope.modifyParent($scope.workPointsIds[pid].parent_id, index);
        };

        $scope.modifyTree = function (tree) {
            angular.forEach(tree, function (e, i) {
                $scope.workPointsModified.push(e);
                $scope.workPointsIds[e.id] = e;
                $scope.workPointsIds[e.id].index = i;
                if (e.children && e.children.length) {
                    $scope.workPointsSums[e.id] = e.children;
                    $scope.modifyTree(e.children);
                } else if (e.trial_balance.length > 0) {
                    e.initial_debit = e.trial_balance[0].initial_debit ? e.trial_balance[0].initial_debit : null;
                    e.initial_credit = e.trial_balance[0].initial_credit ? e.trial_balance[0].initial_credit : null;
                    e.move_debit = e.trial_balance[0].move_debit ? e.trial_balance[0].move_debit : null;
                    e.move_credit = e.trial_balance[0].move_credit ? e.trial_balance[0].move_credit : null;
                    $scope.modifyParent(e.parent_id, 'initial_debit');
                    $scope.modifyParent(e.parent_id, 'initial_credit');
                    $scope.modifyParent(e.parent_id, 'move_debit');
                    $scope.modifyParent(e.parent_id, 'move_credit');
                }
            });
        };

        $scope.modifyTree($scope.workPoints);

        $scope.pMatch = function (id) {
            $scope.workPointsIds[id].match = $scope.workPointsIds[id].match ? !$scope.workPointsIds[id].match : true;
            $scope.$apply();
        };

        $scope.showBtn = function (p) {
            if (p.level === 3) {
                $scope.workPointsIds[p.id].showBtnAdd = $scope.workPointsIds[p.id].showBtnAdd ? !$scope.workPointsIds[p.id].showBtnAdd : true;
            } else if (p.level === 4) {
                $scope.workPointsIds[p.id].showBtnRm = $scope.workPointsIds[p.id].showBtnRm ? !$scope.workPointsIds[p.id].showBtnRm : true;
            }
        };

        $scope.addRow = function (p) {
            if (p.level === 3) {
                $scope.currentIndex++;
                var index = 'null' + $scope.currentIndex;
                $scope.workPointsIds[p.id].children.push({id: index, level: 4, number: null, name: '', parent_id: p.id});
                $scope.workPointsModified = [];
                $scope.modifyTree($scope.workPoints);
            }
        };
        $scope.removeRow = function (p) {
            if (p.level === 4) {
                var level4 = $scope.workPointsIds[p.id];
                var level3 = $scope.workPointsIds[p.parent_id];
                var level2 = $scope.workPointsIds[level3.parent_id];
                var level1 = $scope.workPointsIds[level2.parent_id];

                delete $scope.workPoints[level1.index].children[level2.index].children[level3.index].children[level4.index];
                delete $scope.workPointsIds[p.id];
                $scope.workPointsModified = [];
                $scope.modifyTree($scope.workPoints);
            }
        };

        $.ajaxSetup({
            headers: {
                'X-CSRF-TOKEN': $window.Laravel.csrfToken
            }
        });

        $scope.save = function () {
            $scope.processing = true;
            $scope.error = false;

            var result = {};
            for (var key in $scope.workPointsIds) {
                if ($scope.workPointsIds[key].level === 4)
                    result[key] = $scope.workPointsIds[key];
            }
            var data = JSON.stringify(result);
            $.ajax({
                url: '/client/' + $scope.client.id + '/balance/save',
                type: 'post',
                data: {data: data},
                success: function (data) {
                    $scope.processing = false;
                    console.log(data);
                    if (data.saved)
                        $scope.success = true;
                    else
                        $scope.error = true;
                    $scope.$apply();
                },
                error: function (e) {
                    $scope.processing = false;
                    console.log(e.responseText);
                    $scope.$apply();
                },
            });
        }
    }]);