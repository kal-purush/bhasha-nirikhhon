/// <reference path="../typings/zlib-ts.d.ts" />
///////////////////////////////////////////////////////////////////////////////
// Copyright (c) ENikS.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0  ( the  "License" );  you may 
// not use this file except in compliance with the License.  You may  obtain  a 
// copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required  by  applicable  law  or  agreed  to  in  writing,  software 
// distributed under the License is distributed on an "AS  IS"  BASIS,  WITHOUT
// WARRANTIES OR CONDITIONS  OF  ANY  KIND, either express or implied.  See the 
// License for the specific  language  governing  permissions  and  limitations 
// under the License.

import {Inflate} from "./inflate";



/**
* Inflates deflated archave stored in Iterable<T> 
* @param source An Array, Typed array, String or other Iterable object.
* @example
*  var iterable: Iterable<T> = inflate(arr); 
*/
export function asInflatable<T>(deflated?: Iterable<number>): Iterable<number> {
    if (null === deflated) throw "No Inflate source specidied";
    return new FlateEnumerable(deflated, (iterator: Iterator<number>) => new Inflate(iterator));
}

    
/**
* Inflates deflated archave packaged as zlib stream and stored in Iterable<T> object.
* @param source An Array, Typed array, String or other Iterable object.
* @example
*  var iterable: Iterable<T> = zlibInflate(arr); 
*/
export function zlibInflate<T>(source?: Iterable<T>): Iterable<T> {
    return null;
}



class FlateEnumerable implements Iterable<number> {

    constructor(private _target: Iterable<number>, private _factory: Function) { }

    /** Returns JavaScript iterator */
    public [Symbol.iterator](): Iterator<number> {
        return this._factory(this._target[Symbol.iterator]);
    }

}


class ZLibInflate extends Inflate {
    ///////////////////////////////////////////////////////////////////////////

    public constructor(iterator: Iterator<number>, verify: Function = null) {
        var result: IteratorResult<number>;

        if ((result = iterator.next()).done) {
            throw Inflate.endOfStreamEx;
        }

        var cm = result.value;
        if (0x08 != (cm & 0xf)) {
            throw "unknown compression method";
        }
        if (((cm >> 4) + 8) > 0x0F) {
            throw "invalid window size";
        }

        if ((result = iterator.next()).done) {
            throw Inflate.endOfStreamEx;
        }

        if ((((cm << 8) + result.value) % 31) != 0) {
            throw "incorrect header check";
        }

        super(iterator, (adler32) => {
            var need = 0;      // stream check value
            var byteSignificance = 4;
            var result: IteratorResult<number>;

            while ((0 < byteSignificance) && !(result = iterator.next()).done) {
                switch (byteSignificance) {
                    case 4:
                        need = ((result.value & 0xff) << 24) & 0xff000000;
                        break;

                    case 3:
                        need += ((result.value & 0xff) << 16) & 0xff0000;
                        break;

                    case 2:
                        need += ((result.value & 0xff) << 8) & 0xff00;
                        break;

                    case 1:
                        need += (result.value & 0xff) & 0xff;
                        break;
                }
                byteSignificance--;
            }

            if (need != adler32) {
                throw "Invalid Adler-32 checksum";
            }
        });
    }
}