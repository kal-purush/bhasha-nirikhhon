import {
    Entry, Pair, KoconutOpener,

    KoconutIterable
} from "../../../module.internal"
import { EventEmitter } from "events";

export class Flow<DataType> extends EventEmitter implements Iterable<Entry<number, DataType>>{

    private static newDatumInsertedEvent = "newDatumInserted"
    private static dataScanningCompletedEvent = "dataScanningCompleted"

    private mPentDataSize = 0
    private mChainedFlow : Flow<any> | null = null
    private mInnerDataMap = new Map<number, DataType>();
    [Symbol.iterator]() : Iterator<Entry<number, DataType>> {
        return this.dataEntries[Symbol.iterator]()
    }
    
    constructor(
        srcSequence : Iterable<[number, DataType] | Entry<number, DataType> | Pair<number, DataType>> | null = null
    ) {
        super()
        if(srcSequence != null) {
            for(const eachDatum of srcSequence) {
                this.mPentDataSize++
                if(eachDatum instanceof Entry) this.mInnerDataMap.set(eachDatum.key, eachDatum.value)
                else if(eachDatum instanceof Pair) this.mInnerDataMap.set(eachDatum.first, eachDatum.second)
                else this.mInnerDataMap.set(eachDatum[0], eachDatum[1])
            }
        }
    }

    static of<DataType>(
        ...srcSequence : ([number, DataType] | Entry<number, DataType> | Pair<number, DataType>)[]
    ) : Flow<DataType> {

        return new Flow(srcSequence)

    }

    static from<DataType>(
        srcSequence : Iterable<[number, DataType] | Entry<number, DataType> | Pair<number, DataType>> | null = null
    ) : Flow<DataType> {

        return new Flow(srcSequence)

    }

    static ofSimple<DataType>(
        ...srcSequence : DataType[]
    ) : Flow<DataType> {

        return new Flow(srcSequence.map((eachDatum, eachIndex) => new Entry(eachIndex, eachDatum)))

    }

    static fromSimple<DataType>(
        srcSequence : Iterable<DataType> | null = null
    ) : Flow<DataType> {

        if(srcSequence != null)
            return new Flow(Array.from(srcSequence).map((eachDatum, eachIndex) => new Entry(eachIndex, eachDatum)))
        else return new Flow()

    }

    private sort() : Flow<DataType> {
        this.mInnerDataMap = new Map([...this.mInnerDataMap.entries()].sort())
        return this
    }

    get dataArray() : Array<DataType> {
        return Array.from(this.sort().mInnerDataMap.entries()).map(eachIterableEntry => eachIterableEntry[1])
    }

    get dataEntries() : Array<Entry<number, DataType>> {
        return Array.from(this.sort().mInnerDataMap.entries()).map(eachIterableEntry => new Entry(eachIterableEntry[0], eachIterableEntry[1]))
    }

    private setDatum(
        id : number, 
        datum : DataType
    ) {
        this.mInnerDataMap.set(id, datum)
        this.emit(Flow.newDatumInsertedEvent, id, datum)
    }

    private onNewDatumInserted(
        onNewDatumInsertedListener : (id : number, datum : DataType) => void | Promise<void>,
        targetFlow : Flow<any> | null = null
    ) {

        let count = 0;
        this.mChainedFlow = targetFlow
        const mediatedListener = async (id : number, datum : DataType) => {
            await onNewDatumInsertedListener(id, datum)
            if(this.mPentDataSize - 1 == count++) {
                this.emit(Flow.dataScanningCompletedEvent)
                if(targetFlow != null) {
                    targetFlow.mPentDataSize = targetFlow.mInnerDataMap.size
                    if(targetFlow.mChainedFlow == null) targetFlow.emit(Flow.dataScanningCompletedEvent)
                }
            }
        }
        this.on(Flow.newDatumInsertedEvent, mediatedListener)
        this.once(Flow.dataScanningCompletedEvent, () => this.removeListener(Flow.newDatumInsertedEvent, mediatedListener))
        if(this.mInnerDataMap.size != 0) this.mInnerDataMap.forEach((datum, id) => this.emit(Flow.newDatumInsertedEvent, id, datum))
        
    }

}

export class KoconutFlow<DataType> extends KoconutIterable<Entry<number, DataType>, Entry<number, DataType>, Flow<DataType>, Flow<DataType>> {

    private mIsChained = false

    async validate(data : Flow<DataType> | null) {
        if(data != null) {
            this.combinedDataWrapper = data
        }
    }

    constructor(srcSequence : Iterable<[number, DataType] | Entry<number, DataType> | Pair<number, DataType>> | null = null) {
        super()
        this.data = new Flow(srcSequence)
    }

    static from<DataType>(
        srcSequence : Iterable<[number, DataType] | Entry<number, DataType> | Pair<number, DataType>> | null = null
    ) : KoconutFlow<DataType> {

        return new KoconutFlow(srcSequence)

    }

    static of<DataType>(
        ...srcSequence : ([number, DataType] | Entry<number, DataType> | Pair<number, DataType>)[]
    ) : KoconutFlow<DataType> {

        return new KoconutFlow(srcSequence)

    }

    static fromSimple<DataType>(
        srcSequence : Iterable<DataType> | null = null
    ) : KoconutFlow<DataType> {

        return new KoconutFlow(Flow.fromSimple(srcSequence))

    }

    static ofSimple<DataType>(
        ...srcSequence : DataType[]
    ) : KoconutFlow<DataType> {

        return new KoconutFlow(Flow.ofSimple(...srcSequence))

    }

    map<ResultDataType>(
        transform : (element : DataType) => ResultDataType | Promise<ResultDataType>,
        thisArg : any = null
    ) : KoconutFlow<ResultDataType> {

        this.mIsChained = true
        transform = transform.bind(thisArg)
        const koconutToReturn = new KoconutFlow<ResultDataType>();
        (koconutToReturn as any as KoconutOpener<Flow<ResultDataType>>)
            .setPrevYieldable(this)
            .setProcessor(async () => {
                const processedFlow = new Flow<ResultDataType>()
                if(this.data != null) {
                    this.data['onNewDatumInserted'](
                        async (id, datum) => processedFlow['setDatum'](id, await transform(datum)),
                        processedFlow
                    )
                } else processedFlow.emit(Flow['dataScanningCompletedEvent'])
                return processedFlow
            })
        return koconutToReturn

    }

    async yield() : Promise<Flow<DataType>> {
        return new Promise(async resolve => {
            await this.process()
            if(this.mIsChained) resolve(this.data!)
            else {
                if(this.processor == null) resolve(this.data!['sort']())
                else this.data!.once(Flow['dataScanningCompletedEvent'], () => {
                    resolve(this.data!['sort']())
                })
            }
        })
    }

}