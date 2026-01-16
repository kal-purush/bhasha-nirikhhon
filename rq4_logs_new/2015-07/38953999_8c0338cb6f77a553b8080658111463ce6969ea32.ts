///<reference path="../../typings/qunit/qunit.d.ts" />
///<reference path="../../src/events/EventDispatcher.ts" />
///<reference path="../../src/collections/ArrayList.ts" />

module collections {
    import EventDispatcher = events.EventDispatcher;
    import ArrayList = collections.ArrayList;

    QUnit.test("ArrayList has correct length.", (assert:QUnitAssert) => {

        // Setup
        const collection:ArrayList<Price> = new ArrayList<Price>();

        // Test One, Length should be 0 when initialized with no items.
        assert.equal(collection.length, 0, 'Initial collection should not have a length when no items have been added.');

        // Test two, add two items and ensure length is reflected.
        collection.addItem(new Price(1));
        collection.addItem(new Price(2));
        assert.equal(collection.length, 2, 'collection should have a length of two price objects.');

        // Test three, add an additional one making a total of three.
        collection.addItem(new Price(3));
        assert.equal(collection.length, 3, 'collection should have a length of three after adding a further price object.');

        // Test four, remove an item from the collection.
        collection.removeItemAt(0); // Price(1).
        assert.equal(collection.length, 2, 'collection should have a length of two after the very first price.');

        // Test five, remove everything from the collection.
        collection.removeAll();
        assert.equal(collection.length, 0, 'Collection should have zero items after calling removeAll().');
    });

    QUnit.test("ArrayList has items at correct position.", (assert:QUnitAssert) => {

        // Setup
        const collection:ArrayList<Price> = new ArrayList<Price>();

        // Test One, add two items and ensure they at correct position.
        collection.addItem(new Price(100));
        collection.addItem(new Price(200));
        assert.equal(collection.getItemAt(0).value, 100, 'added item at position should have a value of 100.');
        assert.equal(collection.getItemAt(1).value, 200, 'added item at position should have a value of 200.');

        // Test Two, add one item to previous two items in collection and ensure they are at the correct position.
        collection.addItem(new Price(300));
        assert.equal(collection.getItemAt(2).value, 300, 'added item at position should have a value of 300.');

        // Test Three, add one item at index position 1 (second in the collection).
        collection.addItemAt(new Price(400), 1)
        assert.equal(collection.getItemAt(0).value, 100, 'added item at position should have a value of 100.');
        assert.equal(collection.getItemAt(1).value, 400, 'added item at position should have a value of 400.');
        assert.equal(collection.getItemAt(2).value, 200, 'added item at position should have a value of 200.');

        // Test four, remove item at position 2 (Price with value 200).
        collection.removeItem(collection.getItemAt(2));
        assert.equal(collection.getItemAt(0).value, 100, 'added item at position should have a value of 100.');
        assert.equal(collection.getItemAt(1).value, 400, 'added item at position should have a value of 400.');
        assert.equal(collection.getItemAt(2).value, 300, 'added item at position should have a value of 300.');
    });

    QUnit.test("ArrayList adding entire collection.", (assert:QUnitAssert) => {

        // Setup
        const sourceList:ArrayList<Price> = new ArrayList<Price>([new Price(100), new Price(200), new Price(300)]);
        const targetList:ArrayList<Price> = new ArrayList<Price>();
        targetList.addAll(sourceList);

        // Test One, Length should be 3.
        assert.equal(targetList.length, 3, 'collection should have a length of 3 after adding all items from source list.');
        assert.equal(targetList.getItemAt(0).value, 100, 'added item at position should have a value of 100.');
        assert.equal(targetList.getItemAt(1).value, 200, 'added item at position should have a value of 200.');
        assert.equal(targetList.getItemAt(2).value, 300, 'added item at position should have a value of 300.');
    });

    QUnit.test("ArrayList retrieving item index.", (assert:QUnitAssert) => {

        // Setup
        const priceOne:Price = new Price(100);
        const priceTwo:Price = new Price(200);
        const collection:ArrayList<Price> = new ArrayList<Price>([priceOne, priceTwo]);

        // Test One, retrieve index of first price.
        assert.equal(collection.getItemIndex(priceOne), 0, 'index of first item added to the collection should be 0.');
        assert.equal(collection.getItemIndex(priceTwo), 1, 'index of second item added to the collection should be 1.');
    });

    class Price extends EventDispatcher {
        constructor(value:number) {
            super();
            this._value = value;
        }

        private _value:number;

        public get value():number {
            return this._value;
        }

        public set value(value:number) {
            if (this._value === value)
                return;

            const oldValue:number = this._value;
            this._value = value;
            this.dispatchEvent(new CustomEvent("propertyChange", {
                detail: {
                    property: "value",
                    oldValue: oldValue,
                    newValue: value
                }
            }));
        }
    }
}