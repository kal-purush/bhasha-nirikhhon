//import {collections as coll} from './collection';
//import {collections as iter} from './iterator';

export module collections {
    /**
     * Container that operates in a LIFO (Last-In First-Out) mode,
     * where elements are inserted and extracted only from one end (called 'top') of the container.
     */
    export class Stack<E> /*implements coll.Collection<E>*/ {
        private _elements:Array<E> = [];

        /**
         * Tests whether stack is empty.
         * @returns {boolean} true if stack is empty, false otherwise
         */
        public empty():boolean {
            return this._elements.length == 0;
        }

        /**
         * Returns the current size of the stack.
         * @returns {boolean} true if stack is empty, false otherwise
         */
        public size():number {
            return this._elements.length;
        }

        /**
         * Return the top element from the stack, which is the last element inserted into the stack.
         * @returns {E} the last element inserted into the stack
         */
        public top():E {
            if (this.size() < 1)
                return null;
            return this._elements[this._elements.length - 1];
        }

        /**
         * Inserts the specified element into the stack.
         * @param e the element to be put on top
         */
        public push(e:E) {
            this._elements.push(e);
        }

        /**
         * Removes the element on top of the stack, effectively reducing its size by one.
         *
         * @returns {E} the last element inserted into the stack
         */
        public pop():E {
            if (this.size() < 1)
                return null;
            return this._elements.pop();
        }
    }
}