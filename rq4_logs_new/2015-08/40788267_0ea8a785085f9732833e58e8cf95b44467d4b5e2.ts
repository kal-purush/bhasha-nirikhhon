///<reference path="Action.ts"/>
///<reference path="Task.ts"/>


class GetARandomActionResult implements ITaskResult {
    task_state: TaskState = TaskState.SUCCESS;
    randomNumbers: Array<number>;
}

class GetARandomAction extends Action {

    howMany : number = 1;

    result : GetARandomActionResult = null;

    max : number = 10e6;
    min : number = 0;

    constructor(parent: IAction) {
        super(parent, null);
    }


    get asyncWorker() : ITaskAsyncWorker {return this.workerFn}



    private workerFn(self: GetARandomAction, onSuccess: IResultCallback, onError: IErrorCallback): void {
        var result: GetARandomActionResult = new GetARandomActionResult();

        for (var i = 0; i < self.howMany; i++) {
            result.randomNumbers.push(Math.floor(Math.random() * (this.max - this.min + 1)) + this.min);
        }
        onSuccess(result);
        return;
    }



    applyResult(result: GetARandomActionResult) : void {
        super.applyResult(result);
        this.result = result;
    }
}