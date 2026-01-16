
declare module 'ansi-256-colors'
{
    module Ansi256Colors
    {
        export var fg: Colors;
        export var bg: Colors;
        export var reset :string;

        export interface Colors
        {
            codes: string[];
            standard: string[];
            bright: string[];
            rgb: string[];
            grayscale: string[];

            // get a red-green-blue value by index, in the ranged 0 to 6
            getRgb(r:number, g:number, b:number) :string;
        }
    }

    export = Ansi256Colors;
}