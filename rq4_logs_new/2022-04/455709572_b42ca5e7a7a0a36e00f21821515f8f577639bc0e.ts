export class BudgetCategory {
    public name: string;
    public amount: number;
    public max: number;
    
    constructor(name: string, amount: number, max: number) {
        this.name = name;
        this.amount = amount;
        this.max = max;
    }

    public increase(interval: number = 5):void {
        if ((this.amount + interval > this.max))
            this.amount = this.max;
        else
            this.amount += interval;   
    }
    
    public decrease(interval: number = 5):void {
        if ((this.amount - interval) < 0)
            this.amount = 0;
        else    
            this.amount -= interval;
    }
}

export class Budget {
    public categories:BudgetCategory[];
    public income: number;

    constructor(income: number) {
        this.income = income;
        this.categories = [];
        this.categories.push(new BudgetCategory("Rent", 1000, 1000));
        this.categories.push(new BudgetCategory("Food", 300, income));
        this.categories.push(new BudgetCategory("Phone", 100, 100));
        this.categories.push(new BudgetCategory("Electric", 150, income));
        this.categories.push(new BudgetCategory("Internet", 75, 75));
        this.categories.push(new BudgetCategory("Water", 70, 70));
        this.categories.push(new BudgetCategory("Entertainment", 50, income))

        let total = this.getTotal();
        let extra = income - total;
        if (extra < 0)
            extra = 0;
        this.categories.push(new BudgetCategory("Savings", extra, income));
    }

    public getTotal():number {
        if (!this.categories)
            return 0;

        let total = 0;
        for (let i = 0; i < this.categories.length; i++) {
            total += this.categories[i].amount
        }
        return total;
    }
}