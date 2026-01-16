class Soy extends CondimentDecorator {
    private _beverage: Beverage;

    constructor(beverage: Beverage) {
        super();
        this._beverage = beverage;
    }

    get description(): string {
        return this._beverage.description + ', 豆乳';
    }

    cost(): number {
        var cost = this._beverage.cost();

        switch (this.size) {
            case Size.TALL:
                cost += 0.10;
                break;
            case Size.GRANDE:
                cost += 0.15;
                break;
            case Size.VENTI:
                cost += 0.20;
                break;
        }
        return cost;
    }
}