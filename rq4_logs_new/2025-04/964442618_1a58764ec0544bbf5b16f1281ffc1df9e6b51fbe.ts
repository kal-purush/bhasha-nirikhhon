import BasePopUp from "./BasePopUp";


export type CarModels = {
    Audi: "TT" | "R8" | "Q7" | "A6" | "A8",
    BMW: "3" | "5" | "X5" | "X6" | "Z3",
    Ford: "Fiesta" | "Focus" | "Fusion" | "Mondeo" | "Sierra",
    Porsche: "911" | "Cayenne" | "Panamera",
    Fiat: "Palio" | "Ducato" | "Panda" | "Punto" | "Scudo",
};

export default class AddCarPopUp extends BasePopUp {

    public brandSelect = this.popUp.locator("select[id='addCarBrand']");
    public modelSelect = this.popUp.locator("select[id='addCarModel']");
    public mileageInput = this.popUp.locator("input[id='addCarMileage']");
    public addButton = this.popUp.locator("button", {hasText: "Add"});

    async selectBrand(
        brand: "Audi" | "BMW" | "Ford" | "Porsche" | "Fiat"
    ): Promise<void> {
        await this.brandSelect.selectOption(brand);
    };

    async selectModel(model: string): Promise<void> {
        await this.modelSelect.selectOption(model);
    };

    async selectCar<Brand extends keyof CarModels>(
        brand: Brand,
        model: CarModels[Brand],
        mileage: string | number
    ): Promise<void> {
        await this.selectBrand(brand);
        await this.selectModel(model);
        await this.mileageInput.fill(mileage.toString());
    };
};