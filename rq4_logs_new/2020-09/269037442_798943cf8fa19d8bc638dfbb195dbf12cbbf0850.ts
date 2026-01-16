`use strict`

import {
    Entry, KoconutPrimitive, KoconutOpener,
    KoconutArray, KoconutSet
} from "../../internal"

export class KoconutMap<KeyType, ValueType> extends KoconutPrimitive<Map<KeyType, ValueType>> implements Iterable<Entry<KeyType, ValueType>>{

    /* Iterable */
    [Symbol.iterator]() : Iterator<Entry<KeyType, ValueType>> {
        return this.mEntries[Symbol.iterator]()
    }

    /* Properties */
    private mKeys = new Set<KeyType>()
    private mEntries = new Set<Entry<KeyType, ValueType>>()
    private mValues = new Array<ValueType>() 
    private mSize = 0
    constructor(data : Map<KeyType, ValueType> | null = null) { 
        super(data)
        if(data != null) {
            for(const [key, value] of data.entries()) {
                this.mKeys.add(key)
                this.mEntries.add(new Entry(key, value))
                this.mValues.push(value)
            }
            this.mSize = data.size
        }
    }

    /* Properties Getter */
    keys() : KoconutSet<KeyType> {

        const koconutToReturn = new KoconutSet<KeyType>();
        (koconutToReturn as any as KoconutOpener<Set<KeyType>>)
            .setPrevYieldable(this)
            .setProcessor(async () => this.mKeys)
        return koconutToReturn

    }


    entries() : KoconutSet<Entry<KeyType, ValueType>> {

        const koconutToReturn = new KoconutSet<Entry<KeyType, ValueType>>();
        (koconutToReturn as any as KoconutOpener<Set<Entry<KeyType, ValueType>>>)
            .setPrevYieldable(this)
            .setProcessor(async () => this.mEntries)
        return koconutToReturn

    }


    values() : KoconutArray<ValueType> {

        const koconutToReturn = new KoconutArray<ValueType>();
        (koconutToReturn as any as KoconutOpener<Array<ValueType>>)
            .setPrevYieldable(this)
            .setProcessor(async () => this.mValues)
        return koconutToReturn

    }


    size() : KoconutPrimitive<number>{

        const koconutToReturn = new KoconutPrimitive<number>();
        (koconutToReturn as any as KoconutOpener<number>)
            .setPrevYieldable(this)
            .setProcessor(async () => this.mSize)
        return koconutToReturn

    }

}