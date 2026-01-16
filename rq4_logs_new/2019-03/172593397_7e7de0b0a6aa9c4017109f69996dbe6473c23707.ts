/**
 * @author WMXPY
 * @namespace Hello_Store
 * @description Global Edit Store
 */

import { action, observable } from "mobx";

class GlobalEditDialogStore {

    @observable
    public isOpen: boolean = false;

    @action
    public open(): void {
        this.isOpen = true;
    }

    @action
    public close(): void {
        this.isOpen = false;
    }
}

export const globalEditDialogStore: GlobalEditDialogStore = new GlobalEditDialogStore();