(function () {
    'use strict';
    var scripts = document.getElementsByTagName("script");
    var currentScriptPath = scripts[scripts.length - 1].src;
    angular.module('acAnimate', ['ngRoute'])
        .directive('acAnimate', AcAnimate)
    ;


    /**
     * Directiva que muestra un panel de resultados de las b?squedas. Para darle aspecto, utilizar .ac-result-panel
     * @type {string[]}
     */


    AcAnimate.$inject = ['$injector', '$compile', '$rootScope', '$timeout', '$document'];
    function AcAnimate($injector, $compile, $rootScope, $timeout, $document) {
        return {
            restrict: 'AE',
            scope: {
                animationIn: '@', /*// Animación de entrada*/
                animationOut: '@', /*// Animación de salida*/
                correctionIn: '@', /*// Margen de corrección salida*/
                correctionOut: '@', /*// Margen de corrección de entrada*/
                timing: '@' /*// Timing para la animación*/
            },
            controller: ['$scope', '$element', '$attrs', function ($scope, $element, $attrs) {
                var vm = this;

                $document.bind('scroll', onScroll);


                var latestKnownScrollY = 0,
                    ticking = false;

                /**
                 * Función que se ejecuta efectivamente en el scroll
                 */
                function onScroll() {
                    latestKnownScrollY = window.scrollY;
                    requestTick();
                }

                /**
                 * Se fija si ya está pedido un RAF
                 */
                function requestTick() {
                    if (!ticking) {
                        window.requestAnimationFrame(update);
                    }
                    ticking = true;
                }


                /**
                 * Función que contiene la lógica de ejecución
                 */
                function update() {
                    ticking = false;

                    var currentScrollY = latestKnownScrollY;
                    var rect = $element[0].getBoundingClientRect();

                    var top_adentro = false;
                    var bottom_adentro = false;


                    $scope.correctionOut = ($scope.correctionOut == undefined) ? 0 : $scope.correctionOut;
                    $scope.correctionIn = ($scope.correctionIn == undefined) ? 0 : $scope.correctionIn;

                    var val = rect.top >= (0 - $scope.correctionIn) &&
                        rect.left >= 0 &&
                        rect.bottom <= ((window.innerHeight || document.documentElement.clientHeight) - $scope.correctionOut) && /*or $(window).height() */
                        rect.right <= (window.innerWidth || document.documentElement.clientWidth);
                    /*or $(window).width() */

                    var inOnce = false;


                    //Me fijo si la parte de arriba es visible
                    top_adentro = rect.top <= ((window.innerHeight || document.documentElement.clientHeight) - $scope.correctionOut)
                        && rect.top > 0;

                    //Me fijo si la parte de abajo es visible
                    bottom_adentro = (rect.bottom <= ((window.innerHeight || document.documentElement.clientHeight) - $scope.correctionIn)
                        && rect.bottom > 0) || (rect.top < 0 && rect.bottom > 0);


                    if (top_adentro || bottom_adentro) {
                        if (!$element.hasClass($scope.animationIn)) {
                            $element.addClass($scope.animationIn);
                        }
                    }

                    if (!top_adentro && !bottom_adentro) {
                        $element.removeClass($scope.animationIn);
                    }


                    //if (rect.bottom <= ((window.innerHeight || document.documentElement.clientHeight) - $scope.correctionOut)) {
                    //    if (!$element.hasClass($scope.animationIn)) {
                    //        $element.addClass($scope.animationIn);
                    //    }
                    //} else {
                    //    $element.removeClass($scope.animationIn);
                    //}

                    //if (rect.top >= (0 - $scope.correctionIn)) {
                    //    if (!$element.hasClass($scope.animationOut)) {
                    //        $element.addClass($scope.animationOut);
                    //    }
                    //}else{
                    //    $element.removeClass($scope.animationOut);
                    //}

                }


            }],
            link: function (scope, element, attr) {

            },
            controllerAs: 'acAnimateCtrl'
        };
    }

})();