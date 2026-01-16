module PiStation {
    export class Module {
        functions: PiStation.Function[];
        name: string;

        constructor(name: string, functionArray: PiStation.Function[]) {
            this.functions = functionArray;
        }

        addFunction(func: PiStation.Function) {
            this.functions.push(func);
        }

    }

    export class Function {
        arguments: PiStation.Argument[];
        name: string;

        constructor(name: string, argumentArray: PiStation.Argument[]) {
            this.arguments = argumentArray;
        }

        addFunction(arg: PiStation.Argument) {
            this.arguments.push(arg);
        }

    }

    export class Argument {
        type: string;
        name: string;

        constructor(type: string, name:string) {
            this.type = type;
            this.name = name;
        }
    }
}