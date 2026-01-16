import { Query } from "./Query";
import { Expressions } from "./expressions/expression";
import { CommandResult, Commands } from "./commands/command";
import { CommandProcessor } from "./commands/command-processor";

export abstract class IPersistenceEngine
{
    constructor(private processor: CommandProcessor)
    {

    }

    public abstract load<T>(expression: Expressions): PromiseLike<T[]>;

    public process(...commands: Commands<any>[]): PromiseLike<CommandResult[]>
    {
        return this.processor.visitCommands(commands);
    }
}