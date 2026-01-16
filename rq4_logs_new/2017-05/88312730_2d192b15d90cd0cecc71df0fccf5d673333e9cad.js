/**
 * Created by Good on 5/8/2017.
 */
app.controller('StoreOutputController', function($scope, $http, API, $interval) {

    $http.get(API + 'storage').then(function (response) {
        $scope.stores = response.data;
    });

    $http.get(API + 'product').then(function (response) {
        $scope.products = response.data;
    });

    $http.get(API + 'unit').then(function (response) {
        $scope.units = response.data;
    });

    $http.get(API + 'order').then(function (response) {
        $scope.orders = response.data;
    });

    $http.get(API + 'product-in-store').then(function (response) {
        $scope.productInStores = response.data;
    });

    // LẤY THÔNG TIN ĐƠN HÀNG
    $scope.loadOrder = function(id) {
        $http.get(API + 'order/' + id).then(function (response) {
            $scope.selectedOrder = response.data;
        });
        $http.get(API + 'get-order-detail/' + id).then(function (response) {
            $scope.data = response.data;
        });
    };
    $scope.loadOrder();

    // Thêm sản phẩm vào danh sách
    $scope.add = function(selected) {
        for(var i=0; i<$scope.data.length; i++) {
            if(($scope.data[i].product_id == selected.product_id) && (selected.store_id==$scope.new.store_id)) {
                $scope.data[i].quantity_in_store = selected.quantity;
                $scope.data[i].price_input = selected.price_input;
                $scope.data[i].expried_date = selected.expried_date;
                toastr.info('Đã thêm một sản phẩm vào danh sách');
            }
        };
    };

    // Xóa sản phẩm khỏi danh sách
    $scope.delete = function(selected) {
        console.log(selected);
        console.log($scope.data);
        for(var i=0; i<$scope.data.length; i++) {
            if($scope.data[i].product_id == selected.product_id) {
                $scope.data[i].quantity_in_store = 0;
                $scope.data[i].price_input = 0;
                $scope.data[i].expried_date = null;
                toastr.info('Đã xóa sản phẩm khỏi danh sách.');
            }
        };
    };

    //Load danh sách xuất kho
    $scope.loadStoreOutput = function () {
        $http.get(API + 'store-output').then(function (response) {
            $scope.storeOutputs = response.data;
        });
    };
    $scope.loadStoreOutput();
    $interval($scope.loadStoreOutput, 3000);

    // Thêm chi tiết xuất kho
    $scope.createDetailStoreOutput = function (item) {
        $http({
            method : 'POST',
            url : API + 'detail-store-output',
            data : item,
            cache : false,
            header : {'Content-Type':'application/x-www-form-urlencoded'}
        }).then(function (response) {
            if(response.data.success) {
                console.log('1');
            }
        });
    };

    // Tạo mới xuất kho
    $scope.createStoreOutput = function () {
        if($scope.data.length <= 0)
            toastr.error('Vui lòng chọn đơn hàng');
        else {
            var check = true;
            for (var i=0; i<$scope.data.length; i++) {
                if(($scope.data[i].quantity_in_store - $scope.data[i].quantity) < 0) {
                    check = false; break;
                }
            };
            if (!check)
                toastr.error('Kho không đủ số lượng.' +
                    '\n Vui lòng chọn kho khác, hoặc yêu cầu chuyển kho để lấy thêm hàng.');
            else {
                $scope.new.order_id = $scope.info.order_id;
                $http({
                    method : 'POST',
                    url : API + 'store-output',
                    data : $scope.new,
                    cache : false,
                    header : {'Content-Type':'application/x-www-form-urlencoded'}
                }).then(function (response) {
                    if(response.data.success) {
                        for (var i=0; i<$scope.data.length; i++) {
                            $scope.data[i].store_output_id = response.data.success;
                            $scope.data[i].store_id = $scope.new.store_id;
                            console.log($scope.data[i]);
                            $scope.createDetailStoreOutput($scope.data[i]);
                        }
                        toastr.success('Đã xuất kho thành công.');
                        $scope.data = [];
                        $scope.info = {};
                        $scope.new = {};
                    } else
                        toastr.error(response.data[0]);
                });
            }
        }
    };

    // Xem danh sách xuất kho
    $scope.readStoreOutput = function (storeOutput) {
        $http.get(API + 'store-output/' + storeOutput.id).then(function (response) {
            $scope.selected = response.data;
        });
        $http.get(API + 'get-detail-store-output/' + storeOutput.id).then(function (response) {
            $scope.detail = response.data;
        });
    };

    // Xóa xuất kho
    $scope.deleteStoreOutput = function () {
        $http({
            method : 'DELETE',
            url : API + 'store-output/' + $scope.selected.id,
            cache : false,
            header : {'Content-Type':'application/x-www-form-urlencoded'}
        }).then(function (response) {
            if(response.data.success) {
                toastr.success(response.data.success);
                $("[data-dismiss=modal]").trigger({ type: "click" });
                $scope.loadStoreOutput();
            } else
                toastr.error(response.data[0]);
        });
    };

    // In biểu mẫu
    $scope.print = function () {
        window.print();
    }

});