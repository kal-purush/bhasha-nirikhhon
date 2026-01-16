export declare const HELP: ({
    header: string;
    optionList: ({
        name: string;
        description: string;
        alias: string;
        defaultOption: boolean;
        type: BooleanConstructor;
    } | {
        name: string;
        description: string;
        alias: string;
        defaultOption: boolean;
        type: NumberConstructor;
    } | {
        name: string;
        description: string;
        alias: string;
        defaultOption: boolean;
        type: StringConstructor;
    } | {
        name: string;
        description: string;
        defaultOption: boolean;
        type: StringConstructor;
        alias?: undefined;
    } | {
        name: string;
        description: string;
        alias: string;
        type: BooleanConstructor;
        defaultOption?: undefined;
    })[];
    content?: undefined;
} | {
    header: string;
    content: {
        desc: string;
        example: string;
    }[];
    optionList?: undefined;
})[];
export declare const CONTEXT = "FC_LOGS";