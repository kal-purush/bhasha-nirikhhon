export default class TestsInventory {

    private static inventoryInstance: TestsInventory;
    private methodsList: Array<PropertyDescriptor>;

    private constructor() {
        this.methodsList = [];
    }

    public static instance(): TestsInventory {
        if (!TestsInventory.inventoryInstance) {
            TestsInventory.inventoryInstance = new TestsInventory();
        }

        return TestsInventory.inventoryInstance;
    }

    public methods(){
        return this.methodsList;
    }
}