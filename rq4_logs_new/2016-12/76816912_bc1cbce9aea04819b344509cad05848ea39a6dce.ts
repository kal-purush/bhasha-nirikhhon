/**
 * Created by Daniel Fallon on 12/30/2016.
 */

export interface SetItem {
    hashCode: string
    equals(object: Object);
}

export class OrderedSet<T extends SetItem> extends Array{
    private _uniqueItems:any;
    private _length:number;

    public indexOf(item:T):void{
        this.throwNotImplemented("indexOf");
    }
    public pop():T{
        let output = super.pop() as SetItem;
        delete this._uniqueItems[output.hashCode];
    }
    public push(...items:T[]):void{
        items.map((item)=>{
            this._uniqueItems[item.hashCode] = super.push(item)-1;
        });
    }
    private throwNotImplemented(methodName: string):void{
        throw Error(`method "${methodName}" is not implemented`);
    }
}