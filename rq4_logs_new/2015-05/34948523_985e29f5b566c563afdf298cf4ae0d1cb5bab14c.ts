// TypeScriptではAbstractクラスが作れないので、派生クラス（サブクラス）でオーバーライドする
// Abstrractクラスが、そろそろ実装されるかも https://github.com/Microsoft/TypeScript/issues/6
module Strategy {
    export class Duck {
        private _flyBehavior: IFlyBehavior;
        private _quackBehavior: IQuackBehavior;

        constructor() { }

        // Abstract Method
        display() {
        }

        performFly() {
            this._flyBehavior.fly();
        }

        performQuack() {
            this._quackBehavior.quack();
        }

        set flyBehavior(behavior: IFlyBehavior) {
            this._flyBehavior = behavior;
        }

        set quackBehavior(behavior: IQuackBehavior) {
            this._quackBehavior = behavior;
        }

        swim() {
            console.log('すべての鴨は浮かびます。');
        }
    }
}