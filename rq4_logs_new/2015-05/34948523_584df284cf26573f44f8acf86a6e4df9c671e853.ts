class ChocolateBoiler {
    private empty: boolean;
    private boiled: boolean;
    private static uniqueInstance: ChocolateBoiler = null;

    // 'private' modifier cannot appear on a constructor declaration.
    constructor() {
        this.empty = true;
        this.boiled = false;
    }

    static getInstance(): ChocolateBoiler {
        if (this.uniqueInstance === null) {
            console.log('ChocolateBoilerの唯一のインスタンスをつくる');
            this.uniqueInstance = new ChocolateBoiler();
        }
        console.log('ChocolateBoilerのインスタンスを返す');
        return this.uniqueInstance;
    }

    fill(): void {
        if (this.isEmpty()) {
            this.empty = false;
            this.boiled = false;
        }
    }

    drain(): void {
        if (!this.isEmpty() && this.isBoiled()) {
            this.empty = true;
        }
    }

    boil(): void {
        if (!this.isEmpty() && !this.isBoiled()) {
            this.boiled = true;
        }
    }

    isEmpty(): boolean {
        return this.empty;
    }

    isBoiled(): boolean {
        return this.boiled;
    }
}