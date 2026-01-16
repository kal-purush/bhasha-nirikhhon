
declare module "js-vim"
{
    module JSVim
    {
        export var modeName: string;

        export function addCommand (options:ICommandOptions) :void;
        export function mode (newMode:string) :void;
        export function exec (cmd:string) :void;

        export interface ICommandOptions {
            mode: string;
            match: string|RegExp;
            fn: () => void;
        }

        export interface IDocumentOptions {
            text: string;
        }

        export interface IJSONDocument {
            /** Document contents. */
            text: string;
            /** Cursor position. */
            cursor: number;
            /** Current selection (returned in the format: [startPos, {line: ..., char: ...}] with the object containing `line` and `char` describing the end of the selection). */
            selection: [number, ISelectionEnd];
        }

        export interface ISelectionEnd {
            line: number;
            char: number;
        }

        export class Document extends Event
        {
            undo:      any; // @@TODO: class Undo
            selecting: boolean;
            yanking:   boolean;

            new(options?:IDocumentOptions);

            /** Set the parameters for the last command that was executed (if your command needs to keep track of such a thing). */
            last (key:string, value:any) void;
            set (obj:any);
            set (key:string, value:any) :void;
            text (text:string) :void;
            text () :string;
            // getRange (range)
            /** Returns the contents of the line at the cursor position. */
            line() :string;
            /** Returns the contents of the line at index `pos`. */
            line(pos:number) :string;
            /** Insert text into a document. */
            insert (text:string) :void;
            /** Returns the document's JSON representation. */
            toJSON(): IJSONDocument;
        }
    }

    export = JSVim;
}


