import {
    Entry, KoconutArray, KoconutEquatable
} from "./dist/index"
// https://typedoc.org/guides/doccomments/


class MyKey {
    private keyString : string
    constructor(keyString : string) {
        this.keyString = keyString
    }
}

class MyEquatableKey implements KoconutEquatable {

    private keyString : string
    constructor(keyString : string) {
        this.keyString = keyString
    }
    equalsTo(other : MyEquatableKey) {
        return this.keyString == other.keyString
    }

}

const myKeyEntry = Entry.from([new MyKey("myKeyString"), 0])
const myKeyEntry2 = Entry.from([new MyKey("myKeyString"), 0])
console.log(`${myKeyEntry.equalsTo(myKeyEntry2)}`)
// ↑ false

const myEquatableKeyEntry = Entry.from([new MyEquatableKey("myEquatableKeyString"), 0])
const myEquatableKeyEntry2 = Entry.from([new MyEquatableKey("myEquatableKeyString"), 0])
console.log(`${myEquatableKeyEntry.equalsTo(myEquatableKeyEntry2)}`)
// ↑ true