module Watch {

    /** A mechanism for observing a value on an object */
    export interface Watch {

        /** Sets up a watch on a key */
        watch(
            obj: any, key: string, callback: () => void, depth: number
        ): void;

        /** Removes a watch */
        unwatch( obj: any, key: string, callback: () => void ): void;
    }

    /**
     * An ordered list of watchers. When one of the functions is triggered, it
     * automatically invalidates all of the watchers that come after it
     */
    export class PathBinding {

        /** The list of objects to which watchers have been bound */
        private watches: Array<any> = [];

        /** The handler to invoke when something changes */
        private handler = () => {
            this.connect();
            this.trigger();
        };

        /**
         * @param watch The interface for setting up an observer
         * @param each Calls a function for each value in the keypath
         * @param trigger The callback to invoke when there is a change
         */
        constructor(
            private watch: Watch,
            private each: (callback: (obj: any, key: string) => void) => void,
            public trigger: () => void
        ){
            this.connect();
        }

        /** Hooks up watchers for objects that have changed */
        private connect() {
            if ( !this.watch ) {
                return;
            }

            var i = 0;
            this.each((obj, key) => {

                // If the object at this depth has changed since the last
                // iteration, we need to unbind from the old object
                if ( this.watches[i] && this.watches[i] !== obj ) {
                    this.watch.unwatch(this.watches[i], key, this.handler);
                    this.watches[i] = null;
                }

                if ( !this.watches[i] && obj ) {
                    this.watch.watch(obj, key, this.handler, 0);
                    this.watches[i] = obj;
                }

                i++;
            });
        }
    }
}
