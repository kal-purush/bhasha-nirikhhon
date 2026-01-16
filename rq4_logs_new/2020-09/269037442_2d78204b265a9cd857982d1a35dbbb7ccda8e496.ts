import {
    KoconutPrimitive, KoconutOpener
} from "../../../../module.internal"
import { EventEmitter } from 'events'

export class Sequence<DataType> extends EventEmitter implements Iterable<DataType> {

    private mInnerDataArray = new Array<DataType>();
    private mSize = 0
    private static onNewDataInsertedEvent = "onNewDataInserted"
    private static finishedEvent = "finished";

    [Symbol.iterator]() : Iterator<DataType> {
        return this.mInnerDataArray[Symbol.iterator]()
    }

    get dataArray() : Array<DataType> {
        return this.mInnerDataArray
    }

    constructor(srcSequence : Iterable<DataType> | null = null) {
        super()
        if(srcSequence != null) {
            this.mInnerDataArray = Array.from(srcSequence)
            this.mSize = this.mInnerDataArray.length
        }
    }  

    private finish() {
        this.emit(Sequence.finishedEvent)
    }

    private setDatum(index : number, element : DataType) {
        this.mInnerDataArray[index] = element
        this.emit(Sequence.onNewDataInsertedEvent, index, element)
    }

    onNewDataInserted(
        listener : (index : number, element : DataType) => Promise<void>
    ) : Sequence<DataType> {
        this.mInnerDataArray.forEach(async (eachElement, eachIndex) => listener(eachIndex, eachElement))
        this.on(Sequence.onNewDataInsertedEvent, listener)
        this.once(Sequence.finishedEvent, () => this.removeListener(Sequence.onNewDataInsertedEvent, listener))
        return this
    }
    
}

export class KoconutSequence<DataType> extends KoconutPrimitive<Sequence<DataType>>{

    private mIsChained = false

    onEach(
        action : (element : DataType) => void | Promise<void>,
        thisArg : any = null
    ) : KoconutSequence<DataType> {

        this.mIsChained = true
        action = action.bind(thisArg)
        const koocnutToReturn = new KoconutSequence<DataType>()
        const processedSequence = new Sequence<DataType>();
        (koocnutToReturn as any as KoconutOpener<Sequence<DataType>>)
            .setPrevYieldable(this)
            .setProcessor(async () => {
                if(this.data != null) {
                    processedSequence['mSize'] = this.data['mSize']
                    this.data.onNewDataInserted(async (index, element) => {
                        await action(element)
                        processedSequence['setDatum'](index, element)
                        if(index + 1 == processedSequence['mSize']) this.data!['finish']()
                    })
                }
                return processedSequence
            })
        return koocnutToReturn

    }

    
    async yield() : Promise<Sequence<DataType>> {
        return new Promise(async resolve => {
            await this.process()
            if(this.mIsChained) resolve(this.data!)
            else {
                this.data!.onNewDataInserted(async index => {
                    if(index + 1 == this.data!['mSize']) {
                        this.data!['finish']()
                        resolve(this.data!)
                    }
                })
            }
        })
    }

}