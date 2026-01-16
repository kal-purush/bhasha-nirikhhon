// abstract class
class Pizza {
    _name: string;
    dough: string;
    sauce: string;
    toppings: Array<string> = [];

    prepare(): void {
        console.log(this._name + 'を下処理');
        console.log('生地をこねる……');
        console.log('ソースを追加……');
        console.log('トッピングを追加……');
        this.toppings.forEach(t => console.log(' ' + t));
    }

    bake(): void {
        console.log('350度で25分間焼く');
    }

    cut(): void {
        console.log('ピザを扇形に切り分ける');
    }

    box(): void {
        console.log('PizzaStoreの正式な箱にピザを入れる');
    }

    get name(): string {
        return this._name;
    }

    toString(): string {
        var display = '';
        display += '---- ' + this._name + ' ----\n';
        display += this.dough + '\n';
        display += this.sauce + '\n';
        this.toppings.forEach(t => display += t + '\n');

        return display;
    }
}