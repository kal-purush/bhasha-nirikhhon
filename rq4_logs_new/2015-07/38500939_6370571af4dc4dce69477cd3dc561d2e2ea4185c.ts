class Context {
    dependencyAlpha = new DependencyAlpha("A")
    dependencyBeta = new DependencyBeta("B");
    dependencyGamma = new DependencyGamma("C");
}

class DependencyAlpha {
  constructor(public alpha : String) {}
}

class DependencyBeta {
  constructor(public beta : String) {}
}

class DependencyGamma {
  constructor(public gamma : String) {}
}

interface FirstDependencies {
  dependencyAlpha : DependencyAlpha;
  dependencyBeta : DependencyBeta;
}

export class First {
  constructor(private di : FirstDependencies) {}

  process(){
    console.log(this.di.dependencyAlpha.alpha + " and " + this.di.dependencyBeta.beta);
  }
}

interface SecondDependencies {
  dependencyBeta : DependencyBeta;
  dependencyGamma : DependencyGamma;
}

export class Second {
  constructor(private di : SecondDependencies) {}

  process(){
    console.log(this.di.dependencyBeta.beta + " and " + this.di.dependencyGamma.gamma);
  }
}

let context = new Context();
console.log(context);

let myFirst = new First(context);
myFirst.process();

let mySecond = new Second(context);
mySecond.process();