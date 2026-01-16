// JavaScriptではICompaable<T>.toCompareを実装せず、array.sort(toCompare)とする
class Duck {
    private name: string;
    private _weight: number;

    constructor(name: string, weight: number) {
        this.name = name;
        this._weight = weight;
    }

    toString(): string {
        return this.name + 'の重さは' + this._weight;
    }
    
    get weight():number{
        return this._weight;
    }
}