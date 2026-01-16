/**
 * A module for looking up a value based on a wildcard key match.
 */
module Wildcard {
    // NOTE: This could more efficiently be implemented with a trie, but that
    // kind of optimization can come later.

    /** Key value mapping of the input data */
    class KeyValue<T> {

        /** The lookup key */
        public key: string;

        /** Whether this key/value supports a wildcard match */
        public wildcard: boolean;

        /** Constructor */
        constructor( key: string, public value: T ) {
            var star = key.indexOf('*');
            if ( star === -1 ) {
                this.key = key.toLowerCase();
                this.wildcard = false;
            }
            else if ( star - 1 === key.length ) {
                throw new Error(
                    "Wildcard matches are only support at the end of a key");
            }
            else {
                this.key = key.substring(0, star).toLowerCase();
                this.wildcard = true;
            }
        }

        /** Returns whether this key/value matches */
        public matches ( against: string ): boolean {
            if ( this.wildcard ) {
                return this.key === against;
            }
            else {
                return against.indexOf(this.key) === 0;
            }
        }
    }

    /** Creates a lookup table from an object */
    export function createLookup<T>(
        obj: { [key: string]: T }
    ): (string) => T {

        var matchers: Array<KeyValue<T>> = [];
        for ( var key in obj ) {
            matchers.push( new KeyValue(key, obj[key]) );
        }

        // Sort in length descending order to match the most specific
        matchers.sort((a, b) => { return b.key.length - a.key.length; });

        return function lookup (key: string): T {
            key = key.toLowerCase();
            for ( var i = 0; i < matchers.length; i++ ) {
                if ( matchers[i].matches(key) ) {
                    return matchers[i].value;
                }
            }
        };
    }
}