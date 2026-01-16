export module collections {
    export class Deque<E> {
        private _elements:Array<E> = [];

        // Number of items
        public size():number {
            return this._elements.length;
        }

        public empty():boolean {
            return this._elements.length == 0;
        }

        // Inserts element at back
        public inject(e:E) {
            this._elements.push(e);
        }

        // Returns last element
        public back():E {
            if (this.empty()) throw new Error('Deque is empty');
            return this._elements[this._elements.length - 1];
        }

        // Inserts element at front
        public push(e:E):E {
            this._elements.unshift(e);
            return this.front();
        }

        // Removes first element
        public front():E {
            if (this.empty()) throw new Error('Deque is empty');
            return this._elements[0];
        }

        // Removes last element
        public eject():E {
            if (this.empty()) throw new Error('Deque is empty');
            return this._elements.pop();
        }

        // Remove first element
        public pop():E {
            if (this.empty()) throw new Error('Deque is empty');
            return this._elements.shift();
        }
    }
}