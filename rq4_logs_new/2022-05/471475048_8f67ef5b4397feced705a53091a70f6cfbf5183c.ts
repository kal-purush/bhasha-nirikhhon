import { IOption } from "@/assets/types"

export interface ILimits {
    start: number;
    end: number;
}

export interface IOptionForModel {
    S: number;
    K: number;
    sigma: number;
    r: number;
    T: Date;
}

export enum OptionPrice {
    call = 'колл',
    put = 'пут'
}

export interface OptionModelCache {
    call: Map<string, number>;
    put: Map<string, number>;
}

export interface IOptionModel {
    Option: IOption;
    Cache?: OptionModelCache;

    callPrice(...args: unknown[]): number;
    putPrice(...args: unknown[]): number;
}

export enum Models {
    BlackScholes = 'Модель Блэка-Шоулза',
    CoxRossRubinstein = 'Модель Кокса-Росса-Рубинштейна'
}

export interface IModelComparingGraphSettings {
    limits: ILimits,
    price: OptionPrice[],
    models: Models[]
}