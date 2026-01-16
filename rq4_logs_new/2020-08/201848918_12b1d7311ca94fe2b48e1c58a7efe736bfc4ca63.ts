export declare class BaseEquip {
    Suiter: {};
    SuitStatus: {};
    SuitTypes: {};
    TypeHints: {};
    Converter: any;
    constructor();
    getPrimaryStatusCode(mainDevType: string, status: string): string;
    getSecondaryStatusCode(mainDevType: string, secondarySubType: string, status: string): string;
    getDescriptorByCode(code: string): string;
    getMainDescriptor(mainDevType: string, code: string): string;
    getDescriptors(mainDevType: string, statusBitStr: string, separator?: string): string;
}