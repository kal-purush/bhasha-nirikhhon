import {
    /* Tool */
    KoconutPrimitive, KoconutOpener
} from "../../internal"

export class KoconutIterable<DataType, CombinedDataType, CombinedWrapperType extends Iterable<CombinedDataType>,WrapperType extends Iterable<DataType>> extends KoconutPrimitive<WrapperType> implements Iterable<CombinedDataType> {

    /* Iterable */
    [Symbol.iterator]() : Iterator<CombinedDataType> {

        return (this.combinedDataWrapper as Iterable<CombinedDataType>)[Symbol.iterator]()

    }

    protected combinedDataWrapper : CombinedWrapperType | null = null

    all(
        predicate : (element : CombinedDataType) => boolean | Promise<boolean>,
        thisArg : any = null
    ) : KoconutPrimitive<boolean> {

        predicate = predicate.bind(thisArg)
        const koconutToReturn = new KoconutPrimitive<boolean>();
        (koconutToReturn as any as KoconutOpener<boolean>)
            .setPrevYieldable(this)
            .setProcessor(async () => {
                if(this.combinedDataWrapper == null) return false
                for(const eachCombinedData of this.combinedDataWrapper)
                    if(!await predicate(eachCombinedData)) return false
                return true
            })
        return koconutToReturn

    }


    any(
        predicate : (element : CombinedDataType) => boolean | Promise<boolean>,
        thisArg : any = null
    ) : KoconutPrimitive<boolean> {

        predicate = predicate.bind(thisArg)
        const koconutToReturn = new KoconutPrimitive<boolean>();
        (koconutToReturn as any as KoconutOpener<boolean>)
            .setPrevYieldable(this)
            .setProcessor(async () => {
                if(this.combinedDataWrapper == null) return false
                for(const eachCombinedData of this.combinedDataWrapper)
                    if(await predicate(eachCombinedData)) return true
                return false
            })
        return koconutToReturn

    }


    asIterable() : KoconutIterable<DataType, CombinedDataType, CombinedWrapperType, WrapperType> {

        return this

    }


    // asSequence


    count(
        predicate : ((element : CombinedDataType) => boolean | Promise<boolean>) | null = null,
        thisArg : any = null
    ) : KoconutPrimitive<number> {

        if(predicate) predicate.bind(thisArg)
        const koconutToReturn = new KoconutPrimitive<number>();
        (koconutToReturn as any as KoconutOpener<number>)
            .setPrevYieldable(this)
            .setProcessor(async () => {
                if(this.combinedDataWrapper == null) return 0
                let count = 0
                for(const eachCombinedData of this.combinedDataWrapper) {
                    if(!predicate) count ++
                    else if(await predicate(eachCombinedData)) count++
                }
                return count
            })
        return koconutToReturn

    }


}