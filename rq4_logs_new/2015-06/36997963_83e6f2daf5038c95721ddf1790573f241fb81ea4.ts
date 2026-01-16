///<reference path="../../typings/angularjs/angular.d.ts"/>
///<reference path="lifetable.ts"/>
///<reference path="brushes.ts"/>

var app : ng.IModule = angular.module("lifeApp", []);

interface MyScope extends ng.IScope
{
    table : LifeTable.LifeTable;
    current_brush : Brushes.Brush;
    r : number;
    c : number;
    generation : number;
    isRunning : boolean;
    toroidal : boolean;
    delay : number;
    fgcolor : string;
    bgcolor : string;
    timer : ng.IPromise<any>;
    brushes : Brushes.BrushData[];

    start() : void;
    stop() : void;
    step() : void;
    generate() : void;
    generate_random() : void;
    range(n : number) : number[];
    applyBrush(i : number, j : number) : void;
    select(brushdata : Brushes.BrushData) : void;
    handleKeyPress($event : any) : void;
}

class LifeController
{
    static $inject : string[] = ['$scope', '$timeout'];

    constructor(private $scope : MyScope,
                private $timeout : ng.ITimeoutService)
    {
        $scope.timer = null;
        $scope.delay = 1000;
        $scope.r = 10;
        $scope.c = 10;
        $scope.toroidal = true;
        $scope.isRunning = false;
        $scope.generation = 0;
        $scope.brushes = Brushes.BRUSHES;
        $scope.current_brush = $scope.brushes[0].brush;
        $scope.fgcolor = "#000000";
        $scope.bgcolor = "#ffffff";

        $scope.$watch("timer", val =>
        {
            $scope.isRunning = !!val;
        });

        $scope.stop = () =>
        {
            if ($scope.timer)
            {
                $timeout.cancel($scope.timer);
                $scope.timer = null;
            }
        };

        $scope.generate = () =>
        {
            $scope.generation = 0;
            $scope.stop();
            var tableClass : typeof LifeTable.LifeTable = $scope.toroidal ? LifeTable.LifeTableToroidal : LifeTable.LifeTable;
            $scope.table = new tableClass($scope.r, $scope.c);
        };

        $scope.generate_random = () =>
        {
            $scope.generate();
            for (var i = 0; i < $scope.r; i++)
                for (var j = 0; j < $scope.c; j++)
                    if (Math.random() <= 0.5)
                        $scope.table.setElementAt(i, j, true);
        };

        $scope.range = (n : number) : number[] =>
        {
            var a : number[] = [];
            for (var i = 0; i < n; i++)
                a.push(i);
            return a;
        };

        $scope.start = () =>
        {
            $scope.stop();

            var makeStep = () : void =>
            {
                $scope.generation++;
                $scope.table.advance();
                $scope.timer = $timeout(makeStep, $scope.delay);
            };

            $scope.timer = $timeout(makeStep, $scope.delay);
        };

        $scope.applyBrush = (i : number, j : number) : void =>
        {
            if ($scope.current_brush)
            {
                $scope.current_brush.applyTo(i, j, $scope.table);
            }
        };

        $scope.step = () =>
        {
            $scope.stop();
            $scope.table.advance();
            $scope.generation++;
        };

        $scope.handleKeyPress = ($event : any) : void =>
        {
            if ($event.which >= 49 && $event.which <= 57) // is a digit
            {
                var index : number = $event.which - 49;
                if (index < $scope.brushes.length)
                    $scope.select($scope.brushes[index]);
                return;
            }

            switch ($event.which)
            {
                case 32: // space
                    $scope.step();
                    break;
                case 13: // enter
                    if ($scope.isRunning)
                        $scope.stop();
                    else
                        $scope.start();
                    break;
                case 43: // plus
                    $scope.delay += 100;
                    break;
                case 45: // minus
                    $scope.delay -= 100;
                    if ($scope.delay < 0)
                        $scope.delay = 0;
                    break;
                case 103: // g
                    $scope.generate();
                    break;
                case 114: // r
                    $scope.generate_random();
                    break;
            }
        };

        $scope.select = (data : Brushes.BrushData) =>
        {
            $scope.current_brush = data.brush;
        };

        $scope.generate();
    }
}

app.controller('lifeController', LifeController);

app.directive("adjustWidth", ()=>
{
    return <ng.IDirective> {
        restrict: 'A',
        link: (scope : MyScope, element : ng.IAugmentedJQuery, attrs) =>
        {
            scope.$watch(() => scope.table.getRows(), (val) =>
            {
                for (var i = 1; i <= 6; i++)
                    element.removeClass("cell-" + i);
                var value = Math.ceil(+val / 30);
                element.addClass("cell-" + Math.min(value, 6));
            });
        }
    };
});

app.directive("clickableCell", ['$parse', ($parse : ng.IParseService)=>
{
    return {
        restrict: 'A',
        scope: false,
        link: (scope : any, element : ng.IAugmentedJQuery, attrs) =>
        {
            var compiledOnClick = $parse(attrs.onClick);
            var compiledOnMove = $parse(attrs.onMove);

            element.bind('click', () =>
            {
                scope.$apply(() => compiledOnClick(scope, {}));
            }).bind('mousemove', (event) =>
            {
                if (event.which == 1)
                    scope.$apply(() => compiledOnMove(scope, {}));
            });

            scope.$watch(() => scope.table.getElementAt(scope.i, scope.j), val =>
            {
                if (val)
                    element.css('background-color', scope.fgcolor);
                else
                    element.css('background-color', scope.bgcolor);
            });

            scope.$watch("fgcolor", val =>
            {
                if (scope.table.getElementAt(scope.i, scope.j))
                    element.css('background-color', val);
            });

            scope.$watch("bgcolor", val =>
            {
                if (!scope.table.getElementAt(scope.i, scope.j))
                    element.css('background-color', val);
            });
        }
    };
}]);

function invert(color)
{
    function toHex(v)
    {
        var s = v.toString(16);
        if (s.length == 1)
            s = '0' + s;
        return s;
    }

    var r = 255 - parseInt(color.substring(1, 3), 16);
    var g = 255 - parseInt(color.substring(3, 5), 16);
    var b = 255 - parseInt(color.substring(5, 7), 16);
    return '#' + toHex(r) + toHex(g) + toHex(b);
}

app.directive("invertBorder", () =>
{
    return <ng.IDirective> {
        restrict: 'A',
        scope: false,
        link: (scope : any, element : ng.IAugmentedJQuery, attrs) =>
        {
            scope.$watch("bgcolor", val =>
            {
                element.css("border-color", invert(val));
            });
        }
    };
});