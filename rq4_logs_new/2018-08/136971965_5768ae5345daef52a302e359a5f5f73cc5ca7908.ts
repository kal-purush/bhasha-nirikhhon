declare function parseFind(_levelA: (string | object)[]): string;
declare function getGraphQLValue(value: string | object): string | object | undefined;
declare function objectToString(obj: object): any;
declare class Query {
    fnNameS: string;
    headA: any[];
    aliasS: string;
    bodyS: string;
    constructor(_fnNameS: string, aliasOrFilter?: string | object | undefined);
    filter: (filtersO: object) => this;
    setAlias: (_aliasS: string) => this;
    find(findA: (string | object)[] | object): this;
    toString: () => string;
}