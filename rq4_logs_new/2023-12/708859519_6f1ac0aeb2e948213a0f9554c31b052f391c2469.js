let app = angular.module('MyApplication', []);
app.factory('CalcService', function(){
    
    let operacao = {};

    operacao.somar = function(a, b){
        return (a + b);
    };

    operacao.subtrair = function(a, b){
        return (a - b)
    };

    operacao.multiplicar = function(a, b){
        return (a * b);
    };

    operacao.dividir = function(a, b){
        if(b == 0 || a == 0){
            alert('Valor invalido');
        }
        else{
            return (a / b);
        }
    };

    operacao.raiz = function(a){
        return Math.sqrt(a).toFixed(2); 

    };

    operacao.expo = function(a, b){
        return Math.pow(a, b).toFixed(2);
    };

    return operacao;
    
});

app.controller('MyController', function($scope, CalcService){
    $scope.num1 = '';    $scope.num2 = '';    $scope.resultado = '';
    $scope.informacoesExibidas = false;

    $scope.btnCalculo = function(metodo) {

        let numero1 = parseFloat($scope.num1);
        let numero2 = parseFloat($scope.num2);

        if(metodo == '+'){
            $scope.resultado = CalcService.somar(numero1, numero2);
            $scope.informacoesExibidas = true;
        }

        else if(metodo == '-'){
            $scope.resultado = CalcService.subtrair(numero1, numero2);
            $scope.informacoesExibidas = true;
        }

        else if(metodo == 'X'){
            $scope.resultado = CalcService.multiplicar(numero1, numero2);
            $scope.informacoesExibidas = true;
        }

        else if(metodo == '/'){
            $scope.resultado = CalcService.dividir(numero1, numero2);
            $scope.informacoesExibidas = true;
        }

        else if(metodo == 'Raiz'){
            $scope.resultado = CalcService.raiz(numero1, numero2);
            $scope.informacoesExibidas = true;
        }

        else if(metodo == 'Exp'){
            $scope.resultado = CalcService.expo(numero1, numero2);
            $scope.informacoesExibidas = true;
        }
        
    }

  
})