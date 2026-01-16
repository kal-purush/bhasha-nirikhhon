

declare module 'vim-command-regex'
{
    module VimCommandRegex
    {
        interface IMatch {
            type:string;
            count:number;
            chars:string;
        }

        export function match (str:string) :IMatch;

    }

    export = VimCommandRegex;
}