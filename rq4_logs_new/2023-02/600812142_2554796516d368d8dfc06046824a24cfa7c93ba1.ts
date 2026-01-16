import TestsInventory from "./TestsInventory";

export default class TestMethodRunner {

    private descriptor: PropertyDescriptor;

    constructor(target: object, propertyKey: string, descriptor: PropertyDescriptor) {
        this.descriptor = descriptor;
    }

    public run() {
        try {
            const method = this.descriptor.value;
            TestsInventory.instance().methods().push(method);
            method.apply();
            console.log("Passed", method);
        } catch (exception) {
            // Mark failed test with red color.
            console.log("\x1b[31;7mFailed\x1b[0m", this.descriptor.value, (exception as Error).message);
        }
    }
}