package CoffeeShopSimulator;

import CoffeeShopSimulator.Models.Person;

public interface ICoffeeShop {
    /**
     * An example method that can be used in the UI
     * @param data anything
     */
    void exampleMethodToUseInUI(Object data);

    /**
     * Adds a person to the Coffee Shop
     * @param person an example person
     */
    void handleAddingPersonToCoffeeShop(Person person);

    /**
     * Removes a person from a Coffee Shop
     * @param person
     */
    void handleRemovingPersonFromCoffeeShop(Person person);
}