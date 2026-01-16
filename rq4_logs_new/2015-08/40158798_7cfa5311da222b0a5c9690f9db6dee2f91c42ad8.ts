// A Simple Class
class Simple {
    // property
    myNumber: number;
    // constructor
    constructor(startNumber: number) {
        this.myNumber = startNumber;
    }
    // method
    getMyNumber() {
        return this.myNumber;
    }
}

// New up Simple Class
var simple = new Simple(7);


// A Simple Class with private members
class SimplePrivate {
    // private property
    private myNumber: number;
    // constructor
    constructor(startNumber: number) {
        this.myNumber = startNumber;
    }
    // method
    public getMyNumber() {
        return this.myNumber;
    }
}


// A Simple Class with a Static Property
class SimpleStatic {
    // static property
    static multiplier: number = 10;
    // property
    myNumber: number;
    // constructor
    constructor(startNumber: number) {
        this.myNumber = startNumber;
    }
    // method
    multiplyMyNumberByTen() {
        return this.myNumber * SimpleStatic.multiplier;
    }
}

// A Base Class
class Parent {
    id: string;
    constructor(id: string) {
        this.id = id;
    }

    public getId() {
        return this.id;
    }
}

// Child Class with added function
class Child extends Parent{
    
    constructor(id: string) {
        super(id);
    }
         
    public setId(id: string) {
        this.id = id;
    }
}

// Child Class that overrides a Parent class function
class Grandchild extends Child {
    // new public property
    public idChanged: Boolean;

    constructor(id: string) {
        this.idChanged = false;
        super(id);
    }

    // Sets the id but requires a user confirm it first
    public setId(id: string) {
        if (confirm('Are you sure you want to change this Id?')) {
            this.idChanged = true;
            super.setId(id);
        }
    }
}



// No Accessors
class NoAccessor {
    public id: string;
}

// With Accessors get only
class WithAccessorsGetOnly {
    private _id: string;

    constructor(id: string) {
        this._id = id;
    }

    get id(): string {
        return this._id;
    }
}

// With Accessors get and set
class WithAccessorsGetAndSet {
    private _id: string;

    get id(): string {
        return this._id;
    }

    set id(id: string) {
        if (confirm('Are you sure you want to change this Id?')) {
            this._id = id;
        }
    }
}
