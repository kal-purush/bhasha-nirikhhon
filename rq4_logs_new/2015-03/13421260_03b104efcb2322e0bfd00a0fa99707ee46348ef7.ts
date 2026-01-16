module Kii {
    export class KiiThing {
        id : string;
        public data : any;

        constructor(id : string) {
      	     this.id = id;
	}

        public getId() {
	     return this.id;
	}
		    
	public getPath() {
	    return '/things/' + this.id;
	}
    }
}