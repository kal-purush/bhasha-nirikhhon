module Block {

    /** Blocks are pieces of the document that can be added or removed */
    export interface Block {}

    /** Adds and removes elements based on an expression */
    class IfBlock implements Block {

        /** The fragment standing in for the element */
        private standin: Node;

        /** @inheritdoc Block#constructor */
        constructor( private elem: Node ) {
            this.standin = elem.ownerDocument.createComment("if");
        }

        /** @inheritdoc Modifier#execute */
        execute ( value: any ): void {
            if ( value && !this.elem.parentNode ) {
                this.standin.parentNode.replaceChild(this.elem, this.standin);
            }
            else if ( !value && this.elem.parentNode ) {
                this.elem.parentNode.replaceChild(this.standin, this.elem);
            }
        }

        /** Hooks up the behavior for this block */
        connect(): void {
        }

        /** Disconnects the behavior for this block */
        disconnect(): void {}
    }


    /** Defines the interface for instantiating a Block */
    export interface BlockBuilder {
        new( elem: Node ): Block
    }

    /** Default list of blocks */
    export class DefaultBlocks {
        [key: string]: BlockBuilder;
    }

    DefaultBlocks.prototype = {

        /** Removes elements from the DOM if an expression isn't truthy  */
        'if': IfBlock
    };
}
