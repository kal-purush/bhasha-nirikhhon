
import * as Arrays from "../../util/arrays";

import * as Test from "../framework";

export class TestArrays {
    public static testStack(): boolean {
        let stack: Arrays.Stack<number> = new Arrays.Stack<number>();
        let passed: boolean = true;
        
        stack.push(42);
        stack.push(2016);
        stack.push(26);
        passed = Test.Framework.areEqual(26, stack.pop(), Test.LogLevel.Info, "pop") && passed;
        stack.push(2);
        stack.push(23);
        passed = Test.Framework.areEqual(23, stack.pop(), Test.LogLevel.Info, "pop") && passed;
        passed = Test.Framework.areEqual(2, stack.pop(), Test.LogLevel.Info, "pop") && passed;
        passed = Test.Framework.areEqual(2016, stack.pop(), Test.LogLevel.Info, "pop") && passed;
        passed = Test.Framework.areEqual(42, stack.pop(), Test.LogLevel.Info, "pop") && passed;
        passed = Test.Framework.areEqual(0, stack.length, Test.LogLevel.Info, "empty") && passed;
        
        return passed;
    }
    
    
}