App.controller('RegisterPersonaHumanaController', ['$scope', '$window', '$http', function ($scope, $window, $http) {
    $scope.$parent.header = { title: "Registro de Persona Humana", description: "Crea una cuenta y empieza a crear y administrar el contenido." };
    $scope.showErrorMessage = false;

    $scope.personaHumana = {
        nombre: '',
        apellido: '',
        email: '',
        dni: '',
        cuit: '',
        nacionalidad: '',
        fechaNacimiento: '',
        estadoCivil: '',
        profesion: ''
    }

    $scope.validationOptions = {
        rules: {
            nombre: {
                required: true,
            },
            apellido: {
                required: true,
            },
            dni: {
                required: true,
            },
            cuit: {
                required: true               
            },            
            nacionalidad: {
                required: true
            },
            fechaNacimiento: {
                required: false,
                dataType: Date
            },
            estadoCivil: {
                required: false
            },
            profesion: {
                required: false
            }
        }
    }



    $scope.register = function (form) {
        $scope.showErrorMessage = false;

        if (form.validate()) {
            $scope.helpers.uiLoader('show');

            $http({
                url: '/Account/RegisterPersonaHumana',
                dataType: 'json',
                data: {
                    Nombre: $scope.personaHumana.nombre,
                    Apellido: $scope.personaHumana.apellido,
                    Dni: $scope.personaHumana.dni,
                    Cuit: $scope.personaHumana.cuit,
                    Nacionalidad: $scope.personaHumana.nacionalidad,
                    FechaNacimiento: $scope.personaHumana.fechaNacimiento,
                    EstadoCivil: $scope.personaHumana.estadoCivil,
                    Profesion: $scope.personaHumana.profesion
                },
                method: 'POST',
                headers: {
                    "Content-Type": "application/json"
                }
            }).
            then(function (response) {
                if (response != null && response.data > 0) {
                    $scope.showErrorMessage = true;
                } else {
                    $window.location.href = "/Home/Index";
                }
                $scope.helpers.uiLoader('hide');
            }, function (error) {
                $scope.showErrorMessage = true;
                $scope.helpers.uiLoader('hide');
            });
        }
    }

    $scope.helpers.uiLoader('hide');
}]);