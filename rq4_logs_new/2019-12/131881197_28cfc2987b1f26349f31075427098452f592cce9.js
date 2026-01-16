angular.module("zakApp").controller("kakitanganController", [
    "$scope",
    "$http",
    "$location",
    "SweetAlert",
    function($scope, $http, $location, SweetAlert) {
        $scope.listRekod = {
            list: 0
        }
        $scope.paparPengguna = true
        $scope.formPengguna = false
        $scope.kakitangan = {}
        $scope.button_name = "Daftar Kakitangan"
        $scope.enable_edit = true

        $scope.getListUsers = () => {
            $http({
                url: api_url + "/users/senarai-kakitangan",
                method: "GET"
            }).then(response => {
                $scope.listRekod = response.data.result
                $scope.paparPengguna = true
                $scope.formPengguna = false
                $scope.enable_edit = true
                $scope.kakitangan = {}
            })
        }

        $scope.tambahPengguna = () => {
            $scope.paparPengguna = false
            $scope.formPengguna = true
        }

        $scope.process_daftar_kakitangan = () => {
            var data = $scope.kakitangan
            var method = ($scope.button_name==="Kemaskini Kakitangan") ? "PUT": "POST"
            $http({
                header: {
                    "Content-Type": "application/json"
                },
                url: `${api_url}/users/daftar-kakitangan`,
                method: method,
                data: JSON.stringify(data)
            }).then(response => {
                var message = ($scope.enable_edit) ? "didaftarkan" : "dikemaskini"
                SweetAlert.swal({
                    title: "Berjaya!",
                    text: `Pengguna telah ${message}`,
                    type: "success"
                })
                $scope.getListUsers()
            })
        }

        $scope.kemaskini_kakitangan = (pengguna) => {
            $scope.tambahPengguna()
            $scope.enable_edit = false
            $scope.button_name = "Kemaskini Kakitangan"
            var data = {
                nama_penuh: pengguna.usr_fname,
                nama_pengguna: pengguna.usr_name,
                kata_laluan: '',
                usr_id: pengguna.usr_id
            }
            $scope.kakitangan = data
        }

        $scope.buang_kakitangan = staff_id => {
            SweetAlert.swal(
                {
                    title: "Pengesahan",
                    text:
                        "Adakah anda pasti untuk membuang pengguna ini daripada senarai?",
                    type: "warning",
                    showCancelButton: true,
                    confirmButtonColor: "#3e8f3e",
                    confirmButtonText: "Ya Buang!",
                    cancelButtonText: "Batal!",
                    closeOnConfirm: false,
                    closeOnCancel: false
                },
                function(isConfirn) {
                    if (isConfirn) {
                        $http({
                            url: `${api_url}/users/kakitangan?id=${staff_id}`,
                            method: "DELETE"
                        }).then(response => {
                            SweetAlert.swal({
                                title: "Berjaya!",
                                text:
                                    "Pengguna telah dibuang daripada senarai pengguna!",
                                type: "success"
                            })
                            $scope.getListUsers()
                        })
                    } else {
                        SweetAlert.swal({
                            title: "Pembatalan",
                            text: "Pengguna ini tidak dibuang dari senarai",
                            type: "error"
                        })
                    }
                }
            )
        }
    }
])