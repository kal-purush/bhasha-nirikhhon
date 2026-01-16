declare module Chance {

    interface ChanceStatic {
        Chance(): Chance;
        new(): Chance;
        new(seed: number): Chance;
        new(generator: () => any): Chance;
    }

    interface Chance {

        // Basics
        bool(opts?: Options): boolean;
        character(opts?: Options): string;
        floating(opts?: Options): number;
        integer(opts?: Options): number;
        natural(opts?: Options): number;
        string(opts?: Options): string;

        // Time
        ampm(): string;
        date(): Date;
        date(opts: DateOptions): Date|string;
        hammertime(): number;
        hour(opts?: Options): number;
        millisecond(): number;
        minute(): number;
        month(): string;
        month(opts: Options): Month;
        second(): number;
        timestamp(): number;
        year(opts?: Options): string;

        // Helpers
        capitalize(str: string): string;
        mixin(desc: MixinDescriptor): any;
        pad(num: number, width: number, padChar?: string): string;
        pick<T>(arr: T[]): T;
        pick<T>(arr: T[], count: number): T[];
        set: Setter;
        shuffle<T>(arr: T[]): T[];

        // Miscellaneous
        n<T>(generator: () => T, count: number, opts?: Options);
    }

    // A more rigorous approach might be to produce
    // the correct options interfaces for each method
    interface Options { [id: string]: any; }

    interface DateOptions {
        string?: boolean;
        american?: boolean;
        year?: number;
        month?: number;
        day?: number;
    }

    interface Month {
        name: string;
        short_name: string;
        numeric: string;
    }

    interface MixinDescriptor { [id: string]: () => any; }

    interface Setter {
        (key: 'firstNames', values: string[]): any;
        (key: 'lastNames', values: string[]): any;
        (key: 'provinces', values: string[]): any;
        (key: 'us_states_and_dc', values: string[]): any;
        (key: 'territories', values: string[]): any;
        (key: 'armed_forces', values: string[]): any;
        (key: 'street_suffixes', values: string[]): any;
        (key: 'months', values: string[]): any;
        (key: 'cc_types', values: string[]): any;
        (key: 'currency_types', values: string[]): any;
        <T>(key: string, values: T[]): any;
    }
}

// window.chance
declare var chance: Chance.Chance;

// import Chance = require('chance');
declare var importChance: Chance.ChanceStatic;
declare module 'chance' {
    export = importChance;
}