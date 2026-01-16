class Dictionary<T> {
    private _items: any = {}
    private _idGetter: (item: T) => string = null;

    constructor(items?: Array<T>, private _strict: boolean = false) {
        if (items) {
            this.AddItemsFromArray(items);
        }
    }

    public Get(id: any): T {
        if (id) {
            return this._items[id.toString()] || null;
        }
        return null;
    }

    public Count(): number {
        return Object.keys(this._items).length;
    }

    public Any(): boolean {
        for (var key in this._items) {
            if (this._items.hasOwnProperty(key)) {
                return true;
            }
        }
        return false;
    }

    public ToArrayOfKeyValuePairs(): Array<{ Key: string; Value: T}> {
        var result: Array<{ Key: string; Value: T }> = [];
        for (var key in this._items) {
            if (this._items.hasOwnProperty(key)) {
                result.push({ Key: key, Value: this._items[key] });
            }
        }
        return result;
    }

    public ToObject(): any {
        return this._items;
    }

    public FromArray(items: Array<T>): Dictionary<T> {
        this.RemoveAll();
        this.AddItemsFromArray(items);
        return this;
    }

    public ToArray(sortfunction?: any): Array<T> {
        var result: Array<T> = [];
        for (var key in this._items) {
            if (this._items.hasOwnProperty(key)) {
                result.push(this._items[key]);
            }
        }
        if (sortfunction) {
            return result.sort(sortfunction);
        }
        return result;
    }

    public AddItemsFromArray(items: Array<T>) {
        for (var i = 0, j = items.length; i < j; i++) {
            this.Add(items[i]);            
        }
    }

    public Add(item: T): void {
        this.CheckItem(item);
        var id: any = this.GetIdForItem(item);
        if (!this.ItemAlreadyExists(id)) {
            this._items[id] = item;            
        }
    }

    public AddWithKey(key: any, item: T): void {
        if (!this.ItemAlreadyExists(key)) {
            this._items[key] = item;            
        }
    }

    private ItemAlreadyExists(id: any): boolean {
        if (!this._items[id]) {
            return false;
        }
        if (this._strict) {
            throw new Error("There is already an object in the dictionary with the key: " + id);
        }
        return true;
    }

    public Update(item: T): void {
        this.CheckItem(item);
        this._items[this.GetIdForItem(item)] = item;
    }

    public Remove(item: T): void {
        this.CheckItem(item);
        delete this._items[this.GetIdForItem(item)];
    }

    private CheckItem(item: T): void {
        var idForItem = this.GetIdForItem(item);
        if (idForItem || idForItem === 0) {
            return;
        }
        throw Error("Dictionary: CheckItem: Item must have an Id property");
    }

    private GetIdForItem(item: T): any {
        var id: any = null;
        if (this._idGetter) {
            id = this._idGetter(item);
        } else {
            if (item["Id"] || (item["Id"] === 0)) {
                id = item["Id"];                
            }
        }
        return id;
    }

    public SetIdGetter(idGetter: (item: T) => any) {
        this._idGetter = idGetter;
    }

    public RemoveAll(): void {
        this._items = {};
    }

    public ContainsKey(key: any): boolean {
        return this._items.hasOwnProperty(key);
    }

    public Keys(): Array<any> {
        return Object.keys(this._items);
    }
}

export = Dictionary