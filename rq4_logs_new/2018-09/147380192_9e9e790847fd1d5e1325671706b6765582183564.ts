/**
 * @file file that defines entry point into the module
 */

/**
 * Local imports
 */
import { dispatchActions } from "./src/store/actions/actionConfig";
import store from "./src/store/index";
import { IactionDictionary, IeventAction, IkeyValuePair } from "./src/types/index";

/**
 * third party imports
 */

// NA

/**
 * Interface declarations
 */

interface IunifiedStore {
    // store item
    store: any;
    dispatchActions?(dictKey: string, eventAction: IeventAction): any;
    createReduxStoreObject(): void;

}
interface IconstructorProps {
    // used for dispatching actions on the components
    dictionary: IactionDictionary;
}

/**
 * Global constants
 */
// NA

/* ******************************
 * Class Implementation
 * ---------------------------
 * Documentation
 * ---------------------------
 * *******************************
 */
export default class UnifiedStore implements IunifiedStore {
    /*
     * *********************
     * Static properties
     *  *********************
     */
    public actionDictionary: IactionDictionary;
    public store: any;

    /*
     * *********************
     * Static methods
     *  *********************
     */

    /**
     * @method constructor def
     * @param { IconstructorProps } - props
     */
    constructor(props: IconstructorProps) {
        const { dictionary } = props;
        this.actionDictionary = dictionary;
        this.store = this.createReduxStoreObject();
    }
    /**
     * public  dispatchActions
     * dispatches actions as an action creator
     * @return dispatchAction function calls with the right param
     */
    public dispatchActions = (dictKey: string, eventAction: any): any => {
        return dispatchActions(dictKey, eventAction, this.actionDictionary);
    }
    /**
     * Create store object
     * @return { any } reducer Object
     */
    public createReduxStoreObject = () => {
        const st = store(this.actionDictionary);
        return st;
    }

    /**
     * *********************
     * body methods
     * -----------------
     * Method TOC
     * ------------------------
     * **********************
     */

}