/**
 * Created by KnutSindre on 22.01.2015.
 */

/// <reference path="./SequenceEntry.ts" />

module seq {

    /***
     * Dataset for the sequence.
     */
    export class Sequence {

        entries:SequenceEntry[];

        /***
         * Loads the sequence file from a URL.
         */
        static fromURL(url:string):Sequence {

            var req = new XMLHttpRequest();
            req.open('GET', url, false);
            req.send();

            // load file:
            var obj = JSON.parse(req.responseText);

            // FIXME: error checking!

            return Sequence.fromObject(obj);
        }


        /***
         * Loads the sequence from an object.
         */
        static fromObject(obj:any):Sequence {

            var seq = new Sequence();

            seq.entries = obj.entries.map(
                (entry) => { return SequenceEntry.fromObject(entry) }
            );

            return seq;
        }

    }


}