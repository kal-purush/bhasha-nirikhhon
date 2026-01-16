import {EventArgs} from "./event-args";
import {MovedItemEventArgs} from "./moved-item-event-args";


/**
 * Defines a class that represents the values that changed in a collection.
 */
export class CollectionChangedEventArgs<T> extends EventArgs {

    constructor(sender: any) {
        super(sender);
    }

    /**
     * The items that were added to the collection.
     */
    public addedItems: T[];

    /**
     * The index that the added items were inserted at in the collection.
     */
    public addedItemsIndex: number;

    /**
     * The items that were removed from the collection.
     */
    public removedItems: T[];

    /**
     * The index that the first removed item was at in the collection.
     */
    public removedItemsIndex: number;

    /**
     * The items that were moved.
     */
    public movedItems: MovedItemEventArgs<T>[];
}
