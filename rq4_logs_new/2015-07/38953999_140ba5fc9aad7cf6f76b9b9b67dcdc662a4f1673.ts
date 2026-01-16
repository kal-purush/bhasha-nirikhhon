///<reference path="events/EventDispatcher.ts" />
///<reference path="collections/ArrayList.ts" />

module com.uk.example {
    import EventDispatcher = events.EventDispatcher;
    import ArrayList = collections.ArrayList;

    export class Application {

        constructor() {

            // setup some listeners.
            this._prices.addEventListener("collectionChange", function (event:CustomEvent):void {
                console.log("kind: " + event.detail.kind + ' items: ' + event.detail.items[0].value);
            });

            // initialise with one hundred prices.
            var i:number = 0;
            while (i < 100) {
                this._prices.addItem(new Price(Math.random() * 1000));
                i++;
            } // End of loop.


            // Tick some data.
            setInterval(() => this._prices.getItemAt(Math.round(Math.random() * this._prices.length)).value = 5, 500);
        }

        /**
         *
         * @type {collections.ArrayList<Price>}
         */
        private _prices:ArrayList<Price> = new ArrayList<Price>();

        public get prices():ArrayList<Price> {
            return this._prices;
        }
    }

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