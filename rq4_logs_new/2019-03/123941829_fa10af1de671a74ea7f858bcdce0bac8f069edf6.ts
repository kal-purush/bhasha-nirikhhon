import { IAction, IInjectedStoreOptions, IStore, Store } from "rfluxx";

import { ISelectPageStoreInput, ISelectPageStoreOutput } from "../SelectPage/SelectPageStore";

import { IPageStore } from "../../../src/PageStore";

export interface IEditPageStoreState
{
    isEditing: boolean;
    editedString: string;
}

export interface IEditPageStoreOptions extends IInjectedStoreOptions
{
    pageStore: IPageStore;
}

/**
 * This is the interface by which the store is available in the components.
 * It offers a command to edit the string.
 */
export interface IEditPageStore extends IStore<IEditPageStoreState> {
    updateEditedString: IAction<string>;
    setEditing: IAction<boolean>;
}

export class EditPageStore extends Store<IEditPageStoreState> implements IEditPageStore
{
    public readonly updateEditedString: IAction<string>;
    public readonly setEditing: IAction<boolean>;

    public constructor(private options: IEditPageStoreOptions)
    {
        super({
            ...options,
            initialState: {
                isEditing: false,
                editedString: ""
            }
        });

        // create an action that is observable by the store and subscribe it
        this.updateEditedString = this.createActionAndSubscribe<string>(editedString =>
        {
            this.setState({
                ...this.state,
                editedString
            });
        }, { name: "updateEditedString" });

        this.setEditing = this.createActionAndSubscribe<boolean>(isEditing =>
        {
            this.setState({
                ...this.state,
                isEditing
            });

            // tell the page store that we are editing (or not) and may
            // not be evicted
            this.options.pageStore.setEditMode.trigger(isEditing);
        }, { name: "setEditing" });
    }
}