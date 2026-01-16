namespace YJMCNT {
    /**
     * Counter
     */
    class Counter extends Core.Model {
        private _value:number;
        
        constructor() {
            super();
        }
        
        up (val:number){
            this.value += val;
        }
        
        show (){
            return this._value;
        }
        
        private set value(v : number) {
            this._value = Math.ceil(v);
        }
        
        private get value() : number {
            return this._value;
        }

    }
}