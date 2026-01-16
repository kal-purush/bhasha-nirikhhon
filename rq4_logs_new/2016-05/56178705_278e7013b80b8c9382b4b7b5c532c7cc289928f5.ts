export interface OutputInterface {
    log(msj: string, ...obj: Array<any>): void;
}

export interface InputInterface {

    argv: Array<string>;

    exec: Array<string>;

    flags: InputValueList;

    params: InputValueList;
}

export interface FlagInterface {

    name: string;

    description: string;

    list: Array<string>;

    after(input: InputInterface, output: OutputInterface): void;

    before(input: InputInterface, output: OutputInterface): void;

    parse(input: InputInterface, output: OutputInterface): void;
}

export interface CommandInterface {

    description: string;

    handle(input: InputInterface, output: OutputInterface): void;
}

export type QuickCommandType =
    ((input: InputInterface, output: OutputInterface) => void) &
    { description?: string };

export type CommandType = string | QuickCommandType | CommandInterface;

export type CommandTypeList = {
    [command: string]: CommandType;
}

export type CommnadList = {
    [coommand: string]: CommandInterface;
}

export type InputValueList = {
    [param: string]: any | Array<any>
}

/**
 * ParamInterface
 */
export interface ParamInterface<R> {

    definition: string;

    push(param: string): void;

    get(): R;
}