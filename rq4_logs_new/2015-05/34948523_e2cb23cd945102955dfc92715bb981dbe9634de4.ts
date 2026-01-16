// abstract class
class Pizza {
    _name: string;
    dough: string;
    sauce: string;
    toppings: Array<string> = [];

    get name(): string {
        return this._name;
    }

    prepare(): void {
        console.log('下処理 ' + this._name);
    }

    bake(): void {
        console.log('焼く ' + this._name);
    }

    cut(): void {
        console.log('切り分ける ' + this._name);
    }

    box(): void {
        console.log('箱詰めする ' + this._name);
    }

    toString(): string {
        var display = '';
        display = '---- ' + this._name + ' ----\n';
        display += this.dough + '\n';
        display += this.sauce + '\n';
        this.toppings.forEach(t => display += t + '\n');

        return display;
    }
}