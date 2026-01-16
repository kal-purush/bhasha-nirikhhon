///<reference path="../events/EventDispatcher.ts" />

module collections {

    import IEventDispatcher = events.IEventDispatcher;
    import EventDispatcher = events.EventDispatcher;

    export interface IList<T> {

        //--------------------------------------------------------------------------
        //
        //  Properties
        //
        //--------------------------------------------------------------------------

        //----------------------------------
        //  length
        //----------------------------------

        /**
         *  The number of items in this collection.
         *  0 means no items while -1 means the length is unknown.
         */
        length:number;

        //--------------------------------------------------------------------------
        //
        //  Methods
        //
        //--------------------------------------------------------------------------

        /**
         *  Adds the specified item to the end of the list.
         *  Equivalent to <code>addItemAt(item, length)</code>.
         *
         *  @param item The item to add.
         */
        addItem(item:T):void;

        /**
         *  Adds the item at the specified index.
         *  The index of any item greater than the index of the added item is increased by one.
         *  If the the specified index is less than zero or greater than the length
         *  of the list, a RangeError is thrown.
         *
         *  @param item The item to place at the index.
         *
         *  @param index The index at which to place the item.
         *
         *  @throws RangeError if index is less than 0 or greater than the length of the list.
         */
        addItemAt(item:T, index:number):void;

        /**
         *  Gets the item at the specified index.
         *
         *  @param index The index in the list from which to retrieve the item.
         *
         *  @param prefetch An <code>int</code> indicating both the direction
         *  and number of items to fetch during the request if the item is
         *  not local.
         *
         *  @return The item at that index, or <code>null</code> if there is none.
         *
         *  @throws mx.collections.errors.ItemPendingError if the data for that index needs to be
         *  loaded from a remote location.
         *
         *  @throws RangeError if <code>index &lt; 0</code>
         *  or <code>index >= length</code>.
         */
        getItemAt(index:number):T;

        /**
         *  Returns the index of the item if it is in the list such that
         *  getItemAt(index) == item.
         *
         *  <p>Note: unlike <code>IViewCursor.find<i>xxx</i>()</code> methods,
         *  The <code>getItemIndex()</code> method cannot take a parameter with
         *  only a subset of the fields in the item being serched for;
         *  this method always searches for an item that exactly matches
         *  the input parameter.</p>
         *
         *  @param item The item to find.
         *
         *  @return The index of the item, or -1 if the item is not in the list.
         */
        getItemIndex(item:T):number;

        /**
         *  Notifies the view that an item has been updated.
         *  This is useful if the contents of the view do not implement
         *  <code>IEventDispatcher</code> and dispatches a
         *  <code>PropertyChangeEvent</code>.
         *  If a property is specified the view may be able to optimize its
         *  notification mechanism.
         *  Otherwise it may choose to simply refresh the whole view.
         *
         *  @param item The item within the view that was updated.
         *
         *  @param property The name of the property that was updated.
         *
         *  @param oldValue The old value of that property. (If property was null,
         *  this can be the old value of the item.)
         *
         *  @param newValue The new value of that property. (If property was null,
         *  there's no need to specify this as the item is assumed to be
         *  the new value.)
         *
         *  @see mx.events.CollectionEvent
         *  @see mx.events.PropertyChangeEvent
         */
        itemUpdated(item:T, property:string, oldValue:any, newValue:any):void;

        /**
         *  Removes all items from the list.
         *
         *  <p>If any item is not local and an asynchronous operation must be
         *  performed, an <code>ItemPendingError</code> will be thrown.</p>
         *
         *  <p>See the ItemPendingError documentation as well as
         *  the collections documentation for more information
         *   on using the <code>ItemPendingError</code>.</p>
         */
        removeAll():void;

        /**
         *  Removes the item at the specified index and returns it.
         *  Any items that were after this index are now one index earlier.
         *
         *  @param index The index from which to remove the item.
         *
         *  @return The item that was removed.
         *
         *  @throws RangeError is index is less than 0 or greater than length.
         */
        removeItemAt(index:number):T;

        /**
         *  Places the item at the specified index.
         *  If an item was already at that index the new item will replace it
         *  and it will be returned.
         *
         *  @param item The new item to be placed at the specified index.
         *
         *  @param index The index at which to place the item.
         *
         *  @return The item that was replaced, or <code>null</code> if none.
         *
         *  @throws RangeError if index is less than 0 or greater than length.
         */
        setItemAt(item:T, index:number):T;

        /**
         *  Returns an Array that is populated in the same order as the IList
         *  implementation.
         *  This method can throw an ItemPendingError.
         *
         *  @return The array.
         *
         *  @throws mx.collections.errors.ItemPendingError If the data is not yet completely loaded
         *  from a remote location.
         */
        toArray():Array<T>;
    }

    export class ArrayList<T extends EventDispatcher> extends EventDispatcher implements IList<T> {

        //--------------------------------------------------------------------------
        //
        // Constructor
        //
        //--------------------------------------------------------------------------

        /**
         *
         * @param source
         */
        constructor(source:Array<T>) {
            super();

            this.disableEvents();
            this.source = source;
            this.enableEvents();
            // _uid = UIDUtil.createUID();
        }

        //--------------------------------------------------------------------------
        //
        // Variables
        //
        //--------------------------------------------------------------------------

        /**
         *  @private
         *  Indicates if events should be dispatched.
         *  calls to enableEvents() and disableEvents() effect the value when == 0
         *  events should be dispatched.
         */
        private _dispatchEvents:number = 0;

        private _source:Array<T>;

        public get source():Array<T> {
            return this._source;
        }

        public set source(value:Array<T>) {
            var i:number;
            var len:number;
            if (this._source && this._source.length) {
                len = this._source.length;
                for (i = 0; i < len; i++) {
                    this.stopTrackUpdates(this._source[i]);
                }
            }
            this._source = value ? value : [];
            len = this._source.length;
            for (i = 0; i < len; i++) {
                this.startTrackUpdates(this._source[i]);
            }

            if (this._dispatchEvents === 0) {
                const event:CustomEvent = new CustomEvent("collectionChange", {detail: {type: 'reset'}});
                this.dispatchEvent(event);
            }
        }

        //----------------------------------
        // length
        //----------------------------------

        /**
         *  Get the number of items in the list.  An ArrayList should always
         *  know its length so it shouldn't return -1, though a subclass may
         *  override that behavior.
         *
         *  @return int representing the length of the source.
         */
        public get length():number {
            if (this.source)
                return this.source.length;
            else
                return 0;
        }

        //--------------------------------------------------------------------------
        //
        // Methods
        //
        //--------------------------------------------------------------------------

        /**
         *  Get the item at the specified index.
         *
         *  @param  index the index in the list from which to retrieve the item
         *  @param  prefetch int indicating both the direction and amount of items
         *          to fetch during the request should the item not be local.
         *  @return the item at that index, null if there is none
         *  @throws ItemPendingError if the data for that index needs to be
         *                           loaded from a remote location
         *  @throws RangeError if the index &lt; 0 or index &gt;= length
         */
        public getItemAt(index:number):T {
            if (index < 0 || index >= this.length) {
                throw new Error("The supplied index is out of bounds");
            }

            return this.source[index];
        }

        /**
         *  Place the item at the specified index.
         *  If an item was already at that index the new item will replace it and it
         *  will be returned.
         *
         *  @param  item the new value for the index
         *  @param  index the index at which to place the item
         *  @return the item that was replaced, null if none
         *  @throws RangeError if index is less than 0 or greater than or equal to length
         */
        public setItemAt(item:T, index:number):T {
            if (index < 0 || index >= length) {
                throw new Error("The supplied index is out of bounds");
            }

            const oldItem:T = this.source[index];
            this.source[index] = item;
            this.stopTrackUpdates(oldItem);
            this.startTrackUpdates(item);

            if (this._dispatchEvents == 0) {
                const hasCollectionListener:boolean = this.hasEventListener("collectionChange");
                const hasPropertyListener:boolean = this.hasEventListener("propertyChange");
                var updateInfo:CustomEvent;

                if (hasCollectionListener || hasPropertyListener) {

                    updateInfo = new CustomEvent("propertyChange", {
                        detail: {
                            kind: "update",
                            oldValue: oldItem,
                            newValue: item,
                            property: index
                        }
                    });
                }

                if (hasCollectionListener) {
                    const event:CustomEvent = new CustomEvent("collectionChange", {
                        detail: {
                            kind: "replace",
                            location: index,
                            items: [updateInfo]
                        }
                    })
                    this.dispatchEvent(event);
                }

                if (hasPropertyListener)
                    this.dispatchEvent(updateInfo);
            }
            return oldItem;
        }

        /**
         *  Add the specified item to the end of the list.
         *  Equivalent to addItemAt(item, length);
         *
         *  @param item the item to add
         */
        public addItem(item:T):void {
            this.addItemAt(item, length);
        }

        /**
         *  Add the item at the specified index.
         *  Any item that was after this index is moved out by one.
         *
         *  @param item the item to place at the index
         *  @param index the index at which to place the item
         *  @throws RangeError if index is less than 0 or greater than the length
         */
        public addItemAt(item:T, index:number):void {
            if (index < 0 || index > length) {
                throw new Error("The supplied index is out of bounds");
            }

            this.source.splice(index, 0, item);

            this.startTrackUpdates(item);
            this.internalDispatchEvent("add", item, index);
        }

        /**
         *  @copy mx.collections.ListCollectionView#addAll()
         */
        public addAll(addList:IList<T>):void {
            this.addAllAt(addList, length);
        }

        /**
         *  @copy mx.collections.ListCollectionView#addAllAt()
         */
        public addAllAt(addList:IList<T>, index:number):void {
            var length:number = addList.length;
            for (var i:number = 0; i < length; i++) {
                this.addItemAt(addList.getItemAt(i), i + index);
            }
        }

        /**
         *  Return the index of the item if it is in the list such that
         *  getItemAt(index) == item.
         *  Note that in this implementation the search is linear and is therefore
         *  O(n).
         *
         *  @param item the item to find
         *  @return the index of the item, -1 if the item is not in the list.
         */
        public getItemIndex(item:T):number {
            const n:number = this.source.length;
            for (var i:number = 0; i < n; i++) {
                if (this.source[i] === item)
                    return i;
            }

            return -1;
        }

        /**
         *  Removes the specified item from this list, should it exist.
         *
         *  @param  item Object reference to the item that should be removed.
         *  @return Boolean indicating if the item was removed.
         */
        public removeItem(item:T):boolean {
            const index:number = this.getItemIndex(item);
            const result:boolean = index >= 0;
            if (result)
                this.removeItemAt(index);

            return result;
        }

        /**
         *  Remove the item at the specified index and return it.
         *  Any items that were after this index are now one index earlier.
         *
         *  @param index The index from which to remove the item.
         *  @return The item that was removed.
         *  @throws RangeError if index &lt; 0 or index &gt;= length.
         */
        public removeItemAt(index:number):T {
            if (index < 0 || index >= this.length) {
                throw new Error("The supplied index is out of bounds");
            }

            var removed:T = this.source.splice(index, 1)[0];
            this.stopTrackUpdates(removed);
            this.internalDispatchEvent("remove", removed, index);
            return removed;
        }

        /**
         *  Remove all items from the list.
         */
        public removeAll():void {
            if (this.length > 0) {
                var len:number = length;
                for (var i:number = 0; i < len; i++) {
                    this.stopTrackUpdates(this.source[i]);
                }
                this.source.splice(0, length);
                this.internalDispatchEvent("reset");
            }
        }

        /**
         *  Notify the view that an item has been updated.
         *  This is useful if the contents of the view do not implement
         *  <code>IEventDispatcher</code>.
         *  If a property is specified the view may be able to optimize its
         *  notification mechanism.
         *  Otherwise it may choose to simply refresh the whole view.
         *
         *  @param item The item within the view that was updated.
         *
         *  @param property A String, QName, or int
         *  specifying the property that was updated.
         *
         *  @param oldValue The old value of that property.
         *  (If property was null, this can be the old value of the item.)
         *
         *  @param newValue The new value of that property.
         *  (If property was null, there's no need to specify this
         *  as the item is assumed to be the new value.)
         *
         *  @see mx.events.CollectionEvent
         *  @see mx.core.IPropertyChangeNotifier
         *  @see mx.events.PropertyChangeEvent
         */
        public itemUpdated(item:T, property:string = null, oldValue:any = null, newValue:any = null):void {
            const event:CustomEvent = new CustomEvent("propertyChange", {
                detail: {
                    kind: "update",
                    source: item,
                    property: property,
                    oldValue: oldValue,
                    newValue: newValue
                }
            });
            this.itemUpdateHandler(event);
        }

        /**
         *  Return an Array that is populated in the same order as the IList
         *  implementation.
         *
         *  @return An Array populated in the same order as the IList
         *  implementation.
         *
         *  @throws ItemPendingError if the data is not yet completely loaded
         *  from a remote location
         */
        public toArray():Array<T> {
            return this.source.concat();
        }

        /**
         *  Enables event dispatch for this list.
         *  @private
         */
        private enableEvents():void {
            this._dispatchEvents++;
            if (this._dispatchEvents > 0)
                this._dispatchEvents = 0;
        }

        /**
         *  Disables event dispatch for this list.
         *  To re-enable events call enableEvents(), enableEvents() must be called
         *  a matching number of times as disableEvents().
         *  @private
         */
        private disableEvents():void {
            this._dispatchEvents--;
        }

        /**
         *  Called when any of the contained items in the list dispatch an
         *  ObjectChange event.
         *  Wraps it in a <code>CollectionEventKind.UPDATE</code> object.
         *
         *  @param event The event object for the ObjectChange event.
         *
         *  @langversion 3.0
         *  @playerversion Flash 9
         *  @playerversion AIR 1.1
         *  @productversion Flex 3
         */
        protected itemUpdateHandler(event:CustomEvent):void {
            this.internalDispatchEvent("update", event);
            // need to dispatch object event now
            if (this._dispatchEvents == 0 && this.hasEventListener("propertyChange")) {
                const objEvent:CustomEvent = new CustomEvent("propertyChange", event.detail);
                this.dispatchEvent(objEvent);
            }
        }

        /**
         *  If the item is an IEventDispatcher, watch it for updates.
         *  This method is called by the <code>addItemAt()</code> method,
         *  and when the source is initially assigned.
         *
         *  @param item The item passed to the <code>addItemAt()</code> method.
         */
        protected startTrackUpdates(item:T):void {
            if (!item || !(item instanceof EventDispatcher)) return;

            (<IEventDispatcher> item).addEventListener("propertyChange", this.itemUpdateHandler);
        }

        /**
         *  If the item is an IEventDispatcher, stop watching it for updates.
         *  This method is called by the <code>removeItemAt()</code> and
         *  <code>removeAll()</code> methods, and before a new
         *  source is assigned.
         *
         *  @param item The item passed to the <code>removeItemAt()</code> method.
         */
        protected stopTrackUpdates(item:T):void {
            if (!item || !(item instanceof EventDispatcher)) return;

            (<IEventDispatcher> item).removeEventListener("propertyChange", this.itemUpdateHandler);
        }

        /**
         * @private
         * @param kind
         * @param item
         * @param location
         */
        private internalDispatchEvent(kind:string, item:any = null, location:number = -1):void {

            if (this._dispatchEvents == 0) {
                if (this.hasEventListener("collectionChange")) {
                    var event:CustomEvent = new CustomEvent("collectionChange", {
                        detail: {
                            kind: kind,
                            items: [item],
                            location: location
                        }
                    });
                    this.dispatchEvent(event);
                }

                if (this.hasEventListener("propertyChange") && (kind == "add" || kind == "remove")) {

                    const objEvent:CustomEvent = new CustomEvent("propertyChange", {
                        detail: {
                            property: location,
                            newValue: item,
                            oldValue: item
                        }
                    });
                    this.dispatchEvent(objEvent);
                }
            }
        }


    }
}