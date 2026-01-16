import TestCaseRecord from "./TestCaseRecord";
import {TestCaseStatus} from "./TestCaseStatus";

export default class TestCaseInventory {

    private static inventoryInstance: TestCaseInventory;
    private testCases: Array<TestCaseRecord>;

    private constructor() {
        this.testCases = [];
    }

    public static instance(): TestCaseInventory {
        if (!TestCaseInventory.inventoryInstance) {
            TestCaseInventory.inventoryInstance = new TestCaseInventory();
        }

        return TestCaseInventory.inventoryInstance;
    }

    public keep(record: TestCaseRecord) {
        return this.testCases.push(record);
    }

    public size(): number {
        return this.testCases.length;
    }

    public count(status: TestCaseStatus): number {
        let count = 0;
        this.testCases.forEach((caseRecord) => {
            if (caseRecord.status() == status) {
                count++
            }
        });
        return count;
    }

    public summary(qualityGate= 0) {
        this.testCases.forEach((record) => {
            console.log(record.toString());
        });

        const passedTests = this.count(TestCaseStatus.PASSED);
        console.log("Tests passed: %s of %s", passedTests, this.size());

        if ( passedTests + qualityGate != this.size() ) {
            throw new Error("Nice try!")
        }
        else {
            console.log("Clean!");
        }
    }
}