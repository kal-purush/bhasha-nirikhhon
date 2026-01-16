//  An example of lazy initialization for TypeScript.
//  Author: Alexey Gorshkov
//  Idea: http://metanit.com/sharp/tutorial/20.1.php

/// <reference path="../src/Lazy" />

class Library {
    constructor() {
        console.log("Constructor Library");
    }

    getBook() {
        console.log("Give the book to the reader.")
    }
}

class Reader {
    constructor() {
        console.log("constructor Reader");
    }

    library: Library = new Library();

    readBook() {
        this.library.getBook();
        console.log("Read a paper book.");
    }

    readEBook() {
        console.log("Read an e-book.");
    }
}

class LazyReader {
    constructor() {
        console.log("Constructor LazyReader");
    }

    get library(): Library {
        return this._library.value;
    }

    readBook() {
        this.library.getBook();
        console.log("Read a paper book.");
    }

    readEBook() {
        console.log("Read an e-book.");
    }

    private _library: ag23.Lazy<Library> = new ag23.Lazy<Library>(() => new Library());
}

console.log("    Not Lazy:")
var reader = new Reader();
reader.readEBook();
reader.readBook();

console.log("    Lazy:")
var lazyReader = new LazyReader();
lazyReader.readEBook();
lazyReader.readBook();