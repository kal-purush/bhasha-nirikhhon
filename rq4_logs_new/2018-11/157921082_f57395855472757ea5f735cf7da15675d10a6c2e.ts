import { ShellRunner, ShellRunnerType, IExecResults } from './shellrunner'
import cli, { IAzOptions } from '.';
import { Interface } from 'readline';

export class MockRunnerResponse {

    responses: Array<IExecResults> = []

    public Add(response: IExecResults): MockRunnerResponse {

        this.responses = this.responses.concat(response)
        return this
    }

    public GetNext(): IExecResults {
        let t = this.responses.shift() || <IExecResults>{ 
            code: 100,
            stderr: "Mock ExecResults not set"
        }
        return t;
    }

    public Clear(): void {
        this.responses = []
    }
}

//create local scoped global object to hold pending mock responses
let _currentMockResponse = new MockRunnerResponse() 

/** Predefined mock response types for simple mock setup */
export enum MockResponseTypes {

    /** code:0, stdout: 'azure-cli (2.0.0)' */
    version,
    /** code:0 */
    justReturnCode,
    /** code:1, stderr: 'mock error response'  */
    failCode
}

export interface MockResponseFunctions {

    AddResponse(response: IExecResults): cli
    ClearResponseList(): cli
    AddMockResponse(type: MockResponseTypes): cli 
}

export interface MockResponse {
    cli: cli,
    mr: MockResponseFunctions
}

export function MockResponse(options: IAzOptions): MockResponse {
    var _cli: cli
    const self: MockResponseFunctions = {
        /** Add a shell runner result object to the stack of results MockRunner will return with calls to exec() */
        AddResponse : (response: IExecResults): cli => {
            _currentMockResponse.Add(response)
            return _cli
        },

        /** Clear the current response list of result object. Good to do before each test if you rebuild the mock */
        ClearResponseList : (): cli => {
            _currentMockResponse.Clear()
            return _cli
        },

        /** Create pre-canned responses for different types of expected results */
        AddMockResponse: (type: MockResponseTypes): cli => {
            switch(type){
                case MockResponseTypes.version: {
                    self.AddResponse(<IExecResults>{code: 0, stdout: 'azure-cli (2.0.0)\r\n\r\n' })
                    break;
                }
                case MockResponseTypes.justReturnCode: {
                    self.AddResponse(<IExecResults>{code: 0 })
                    break;
                }
                case MockResponseTypes.failCode: {
                    self.AddResponse(<IExecResults>{code: 1, stderr: 'mock error response' })
                    break;
                }
            }
            return _cli
        }
    }

    _currentMockResponse.Add(<IExecResults>{code: 0, stdout: 'azure-cli (2.0.0)\r\n\r\n' })
    _cli = new cli(options)

    return {cli: _cli, mr: self}
}

/** Mock implementation of the ShellRunner interface. This will pop results off the AddResponse*() stack and return them directly */
export class MockRunner extends ShellRunner {
    constructor(shellPath: string){
        super(shellPath)
    };

    public clear(): void {
    }

    public arg(val: string | string[]): ShellRunner {
        return this
    }

    public line(val: string): ShellRunner {
        return this
    }

    public argIf(condition: ()=>boolean, val: string | string[] ): ShellRunner {
        return this
    }

    public exec() : IExecResults {
        return _currentMockResponse.GetNext()
    }
}