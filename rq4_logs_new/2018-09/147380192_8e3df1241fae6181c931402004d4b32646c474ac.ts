/**
 * @file file that defines entry point into the module
 */
import { IactionDictionary, IeventAction } from "./types/index";
/**
 * third party imports
 */
/**
 * Interface declarations
 */
interface IunifiedStore {
    store: any;
    dispatchActions?(dictKey: string, eventAction: IeventAction): any;
    createReduxStoreObject(): void;
}
interface IconstructorProps {
    dictionary: IactionDictionary;
}
/**
 * Global constants
 */
export default class UnifiedStore implements IunifiedStore {
    actionDictionary: IactionDictionary;
    store: any;
    /**
     * @method constructor def
     * @param { IconstructorProps } - props
     */
    constructor(props: IconstructorProps);
    /**
     * public  dispatchActions
     * dispatches actions as an action creator
     * @return dispatchAction function calls with the right param
     */
    dispatchActions: (dictKey: string, eventAction: any) => any;
    /**
     * Create store object
     * @return { any } reducer Object
     */
    createReduxStoreObject: () => void;
}
export {};