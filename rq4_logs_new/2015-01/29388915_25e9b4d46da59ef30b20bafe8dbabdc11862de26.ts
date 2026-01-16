 /// <reference path="JsGrid.ts" />

module JsGrid {
    export type CellDefinition = {
        content: string;
    }

    export class Cell{
        private content: string;

        /*
         * Generates a new Cell
         * @definition  string              The (html-)content of the cell. The content is not html-escaped
         *              CellDefinition      Contains detailed information on how to generate the cell
         */
        constructor(definition?: string|CellDefinition) {
            if (!definition){               //null / undefined -> empty cell
                this.content = "";
            }else
            if (typeof definition === "string"){
                this.content = definition;
            }else{
                this.content = definition.content;
            }
        }


        /*
         * Converts the Cell into an object. Used for serialisation.
         * Performs a deepCopy.
         * @return  mixed      Cell-Representation
         */
        toObject(): any {            
            return this.content;
        }
        
        //Todo: don't provide the following function - either imporove the constructor, or make a factory method
        fromObject(rowId: string, representation: any): void {
            assert(false, "not implemented yet");
        }
    }
}