import dl = require("./DataLayer");
import q = require("q");
export declare class DataContext {
    dataLayer: dl.IDataLayer;
    private _dataModels;
    private _hasFinalizeDone;
    constructor(dataLayer: dl.IDataLayer);
    createDataModel(table: dl.ITable, baseModel?: DataModel): DataModel;
    getDataModel(table: dl.ITable): DataModel;
    finalizeInitialize(): q.Promise<any>;
    hasFinalizeDone(): boolean;
    private addRelationInfoToTableInfo();
    private createTableInfo(table, baseModel?);
    private getNonAbstractDataModels();
    private inheritTableFromBaseModel(table, baseModel);
    private updateSchema();
    private validateTable(table);
}
export declare class DataModel {
    dataContext: DataContext;
    tableInfo: dl.ITableInfo;
    private _dataLayer;
    private _fixedWhere;
    private _beforeInsertCallbacks;
    private _afterInsertCallbacks;
    private _beforeUpdateCallbacks;
    private _afterUpdateCallbacks;
    private _beforeDeleteCallbacks;
    private _afterDeleteCallbacks;
    constructor(dataContext: DataContext, tableInfo: dl.ITableInfo);
    /** Add before insert callback */
    onBeforeInsert(callback: (item: any) => q.Promise<any>): void;
    /** Add after insert callback */
    onAfterInsert(callback: (item: any) => q.Promise<any>): void;
    /** Add before update callback */
    onBeforeUpdate(callback: (item: any) => q.Promise<any>): void;
    /** Add after update callback */
    onAfterUpdate(callback: (item: any) => q.Promise<any>): void;
    /** Add before delete callback */
    onBeforeDelete(callback: (item: any) => q.Promise<any>): void;
    /** Add after delete callback */
    onAfterDelete(callback: (item: any) => q.Promise<any>): void;
    /** Insert the new item into the database */
    insert(itemToCreate: any): q.Promise<any>;
    /** Insert the new item into the database and returns the inserted item */
    insertAndSelect(itemToCreate: any): q.Promise<any>;
    /** Updates the item in the database */
    update(itemToUpdate: any): q.Promise<any>;
    /** Updates the item in the database and returns the updated item */
    updateAndSelect(itemToUpdate: any): q.Promise<any>;
    /** Updates the item in the database */
    updateItems(valuesToUpdate: any, where: any): q.Promise<any>;
    /** Updates the item in the database, if it exists, otherwise creates the item */
    updateOrInsert(item: any): q.Promise<any>;
    /** Updates the item in the database, if it exists, otherwise creates the item and returns the newly read item */
    updateOrInsertAndSelect(item: any): q.Promise<any>;
    /** Deletes the item in the database */
    delete(itemToDelete: any): q.Promise<any>;
    /** Selects the item by its id */
    selectById(id: any): q.Promise<any>;
    /** Selects items from the database by using the selectOptions */
    select(selectOptions: dl.ISelectOptions): q.Promise<any[]>;
    /** appends a fixed where which will be executed always when reading data by operators */
    appendFixedWhere(where: any): void;
    /** returns the column by its name */
    getColumn(columnName: string): dl.IColumn;
    private createCustomSelectOptions(selectOptions);
    private getBaseTables();
    private getCombinedWhere(customWhere);
    private expand(expand, rows);
    private saveChildRelations(row);
    private executeTrigger(itemToChange, eventVariable);
}