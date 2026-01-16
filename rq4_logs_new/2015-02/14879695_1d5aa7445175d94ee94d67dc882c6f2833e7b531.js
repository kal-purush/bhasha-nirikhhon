'use strict';
angular.module('bolao.acordeon', [])
.directive('acordeon', function() {
    return {
        restrict: 'E',
        transclude: true,
        //             replace: true,
        templateUrl: 'ui/acordeon/diretivas/diretiva-acordeon.html',
        scope: true,
        controller: function($scope) {
            
            var eu = this,
            indiceAtual = 0;

            $scope.itens = [];            
            
            eu.adicionarItem = function(item) {
                $scope.itens.push(item);
            };
            
            eu.abrir = function(indice) {                
                if(indice === indiceAtual) {
                    if($scope.itens[indiceAtual].visivel) {
                        $scope.itens[indice].ocultar();
                    }
                    else {
                        $scope.itens[indice].exibir();
                    }
                }
                else {
                    $scope.itens[indiceAtual].ocultar();
                    $scope.itens[indice].exibir();                    
                }
                indiceAtual = indice;
            };          
        }
    };
})
.directive('item', function() {
    return {
        require: '^acordeon',
        restrict: 'E',
        transclude: true,
        scope: {
            boleiro: '=',
            indice: '@'
        },
        link: function(scope, element, attrs, acordeonControle) {
            scope.direcao = "esquerda";

            scope.exibir = function(direcao) {
                scope.visivel = true;
                scope.situacao = "item-aberto";
            }
            
            scope.abrir = function(indice) {                
                acordeonControle.abrir(indice);
            }
            
            scope.ocultar = function() {
                scope.visivel = false;
                scope.situacao = "item-fechado";
            }
            
            scope.ocultar();
            acordeonControle.adicionarItem(scope);
        },
        templateUrl: 'ui/acordeon/diretivas/diretiva-item.html'
    };
});