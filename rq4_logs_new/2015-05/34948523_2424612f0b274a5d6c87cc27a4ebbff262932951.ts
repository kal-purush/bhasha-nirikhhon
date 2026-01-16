class ChicagoIngredientFactory implements IPizzaIngredientFactory {
    createDough(): IDough {
        return new ThickCrustDough();
    }

    createSauce(): ISauce {
        return new PlumTomatoSauce();
    }

    createCheese(): ICheese {
        return new MozzarellaCheese();
    }

    createVeggies(): IVeggies[] {
        var veggies = [
            new BlackOlives(),
            new Spinach(),
            new Eggplant()
        ];

        return veggies;
    }

    createPepperoni(): IPepperoni {
        return new SlicedPepperoni();
    }

    createClam(): IClams {
        return new FrozenClams();
    }
}