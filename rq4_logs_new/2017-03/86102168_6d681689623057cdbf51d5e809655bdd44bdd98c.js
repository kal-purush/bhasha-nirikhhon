var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
function ClassDecorator(target // The class the decorator is declared on
    ) {
    console.log("ClassDecorator called on: ", target);
}
var ClassDecoratorExercise = (function () {
    function ClassDecoratorExercise() {
    }
    ClassDecoratorExercise = __decorate([
        ClassDecorator
    ], ClassDecoratorExercise);
    return ClassDecoratorExercise;
}());
function ClassDecoratorParams(param1, param2) {
    return function (target // The class the decorator is declared on
        ) {
        console.log("ClassDecoratorParams(" + param1 + ", '" + param2 + "') called on: ", target);
    };
}
var ClassDecoratorParamsExe = (function () {
    function ClassDecoratorParamsExe() {
    }
    ClassDecoratorParamsExe = __decorate([
        ClassDecoratorParams(1, "a"),
        ClassDecoratorParams(2, "b")
    ], ClassDecoratorParamsExe);
    return ClassDecoratorParamsExe;
}());
function eat(target // The class the decorator is declared on
    ) {
    console.log("Eat called on: ", target);
}
var Eat = (function () {
    function Eat() {
    }
    Eat = __decorate([
        ClassDecorator
    ], Eat);
    return Eat;
}());
function ClassDecoratorplay(par, par1) {
    return function (target // The class the decorator is declared on
        ) {
        console.log("ClassDecoratorParams(" + par + ", '" + par + "') called on: ", target);
    };
}
var ClassDecoratorParamsplay = (function () {
    function ClassDecoratorParamsplay() {
    }
    ClassDecoratorParamsplay = __decorate([
        ClassDecoratorParams(1, "a"),
        ClassDecoratorParams(2, "b")
    ], ClassDecoratorParamsplay);
    return ClassDecoratorParamsplay;
}());