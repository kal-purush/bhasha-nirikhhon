"use strict";
import parsimmon = require("parsimmon");
import IndentResult = require("./IndentResult");

class ScenarioResult<T> implements parsimmon.Result<T> {
    private result: parsimmon.Result<IndentResult<T>>;
    
    get value() {
        return this.result.value.value;
    }
    
    get status() {
        return this.result.status;
    }
    
    get index() {
        return this.result.index;
    }
    
    get expected() {
        return this.result.expected;
    }
    
    constructor(res: parsimmon.Result<IndentResult<T>>) {
        this.result = res;
    }
}
export = ScenarioResult;