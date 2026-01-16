export default class JserCommandHistory {
    
    private __commands__: string[];
    private __index__: number;
    private __limit__: number;
    
    constructor(limit: number = 50) {
        this.__commands__ = [];
        this.__index__ = -1;
        
        this.__limit__ = limit;
    }
    
    public get(idx: number = this.__index__): string {
        if (~idx) {
            return this.__commands__[this.__index__];
        } else {
            return '';
        }
    }
    
    public get index(): number {
        return this.__index__;
    }
    
    public get first(): string {
        return this.get(0);
    }
    
    public get last(): string {
        return this.get(this.__commands__.length - 1);
    }
    
    public get next(): string {
        let idx = this.__index__;
        let limit = this.__commands__.length;
        
        // moves the index to the next one
        this.__index__ = idx < limit ? idx + 1 : limit;
        
        return this.get();
    }
    
    public get previous(): string {
        let idx = this.__index__;
        
        // moves the index to the previous one
        this.__index__ = idx > 0 ? idx - 1 : 0;
        
        return this.get();
    }
    
    public add(cmd: string): void {
        
        // we skip storing the command if it's empty
        if (!cmd) return;
        
        // we skip storing the command if it's the same as the last one
        if (cmd === this.last) return;
        
        this.__commands__.push(cmd);
        
        if (this.__commands__.length > this.__limit__) {
            // we've surpassed the limit, let's delete the first command
            this.__commands__.shift();
        }
        
    }
    
    public destroy() {
        this.__commands__.length = 0;
    }
    
}