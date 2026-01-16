module List {
  export class Controller {
    scope: any;

    constructor($scope: angular.IScope) {
      this.scope = $scope;
      this.scope.save = new Array<Home.CounterDate>();
      let tmp = {
        begin: new Date(),
        end: new Date(),
        displayed: 0
      }
      for (let i = 1; i < 20; i++)
        this.scope.save.push(tmp);
    }
  }
}