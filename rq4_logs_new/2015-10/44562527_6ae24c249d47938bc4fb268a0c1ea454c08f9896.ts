export default class JserHistory {
    
    private __commands__: string[];
    private __index__: number;
    
    constructor() {
        this.__commands__ = [];
    }
    
    public get command(idx: number = this.__index__) {
        if (~idx) {
            return this.__commands__[this.__index__];
        }
    }
    
    public get index() {
        return this.__index__;
    }
    
    public get first() {
        this.command(0);
    }
    
    public get last() {
        this.command(this.__commands__.lenght - 1);
    }
    
    public get next() {
        let idx = this.__index__;
        let limit = this.__commands__.length;
        this.__index__ = idx < limit ? idx + 1 : limit;
        return this.command();
    }
    
    public get previous() {
        let idx = this.__index__;
        this.__index__ = idx > 0 ? idx - 1 : 0;
        return this.getCommand();
    }
    
    public add(cmd: string) {
        
    }
    
}