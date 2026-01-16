class CaffeineBeverageWithHook {
    //  sealed method
    prepareRecipe(): void {
        this.boilWater();
        this.brew();
        this.pourInCup();
        if (this.customerWantsCondiments()) {
            this.addCondiments();
        }
    }

    // abstract method
    brew(): void { }
    addCondiments(): void { }

    boilWater(): void {
        console.log('お湯を沸かします');
    }

    pourInCup(): void {
        console.log('コップに注ぎます');
    }

    // override
    customerWantsCondiments(): boolean {
        return true;
    }
}