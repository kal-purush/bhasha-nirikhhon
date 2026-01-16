import { Lifetime } from "./Lifetime";
import { createTestProperty, createTestLifetime } from "./testHelpers";

let lifetime = createTestLifetime();

beforeEach(() => {
    lifetime = createTestLifetime();
})

it("can be instantiated", () => {
    expect(lifetime).toBeInstanceOf(Lifetime);
})

it("creates an array with the correct number of years", () => {
    expect(lifetime.lifetimeData).toHaveLength(40)
})

it("reduces mortgage payments to 0 after the mortgage term is reached", () => {
    expect(lifetime.lifetimeData[35].newbuildHouseMortgageYearly).toBe(0);
    expect(lifetime.lifetimeData[34].marketLandMortgageYearly).toBe(0);
    expect(lifetime.lifetimeData[33].fairholdLandMortgageYearly).toBe(0);
    expect(lifetime.lifetimeData[32].marketLandMortgageYearly).toBe(0);
})

describe("resale values", () => {
    it("correctly calculates for a newbuild house", () => {
        // Test newbuild (age 0)
        lifetime = createTestLifetime({
            property: createTestProperty({ age: 0 })
        });
        expect(lifetime.lifetimeData[0].depreciatedHouseResaleValue).toBe(186560);
    });
    it("correctly calculates for a 10-year old house", () => {    // Test 10-year-old house
        lifetime = createTestLifetime({
            property: createTestProperty({ 
                age: 10,
                newBuildPricePerMetre: 2120,
                size: 88 
            })
        }); 
        // Calculate expected depreciation running `calculateDepreciatedBuildPrice()` method on its own
        const houseY10 = createTestProperty({ 
            age: 10,
            newBuildPricePerMetre: 2120,
            size: 88 
        })           
        const depreciatedHouseY10 = houseY10.calculateDepreciatedBuildPrice()
        expect(lifetime.lifetimeData[0].depreciatedHouseResaleValue).toBe(depreciatedHouseY10);
    });
    it("depreciates the house over time", () => {
        // Test value changes over time
        expect(lifetime.lifetimeData[0].depreciatedHouseResaleValue).toBeGreaterThan(
            lifetime.lifetimeData[10].depreciatedHouseResaleValue);
    })
});


it("correctly ages the house", () => {
    lifetime = createTestLifetime({
        property: createTestProperty({ age: 10 }) 
    })

    expect(lifetime.lifetimeData[0].houseAge).toBe(10);
    expect(lifetime.lifetimeData[5].houseAge).toBe(15);
    expect(lifetime.lifetimeData[20].houseAge).toBe(30);
    expect(lifetime.lifetimeData[39].houseAge).toBe(49);
})