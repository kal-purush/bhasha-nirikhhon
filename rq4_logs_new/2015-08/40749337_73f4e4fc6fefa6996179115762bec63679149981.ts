/// <reference path="DataLayer.d.ts" />
/// <reference path="Helpers.d.ts" />
import dl = require("./DataLayer");
import q = require("q");
export declare module ormso {
    class DataContext {
        dataLayer: dl.ormso.IDataLayer;
        private _dataModels;
        private _hasFinalizeDone;
        constructor(dataLayer: dl.ormso.IDataLayer);
        createDataModel(table: dl.ormso.ITable, baseModel?: DataModel): DataModel;
        getDataModel(table: dl.ormso.ITable): DataModel;
        finalizeInitialize(): q.Promise<any>;
        hasFinalizeDone(): boolean;
        private addRelationInfoToTableInfo();
        private createTableInfo(table, baseModel?);
        private getNonAbstractDataModels();
        private inheritTableFromBaseModel(table, baseModel);
        private updateSchema();
        private validateTable(table);
    }
    class DataModel {
        dataContext: DataContext;
        tableInfo: dl.ormso.ITableInfo;
        private _dataLayer;
        private _fixedWhere;
        private _beforeInsertCallbacks;
        private _afterInsertCallbacks;
        private _beforeUpdateCallbacks;
        private _afterUpdateCallbacks;
        private _beforeDeleteCallbacks;
        private _afterDeleteCallbacks;
        constructor(dataContext: DataContext, tableInfo: dl.ormso.ITableInfo);
        onBeforeInsert(callback: (item: any) => q.Promise<any>): void;
        onAfterInsert(callback: (item: any) => q.Promise<any>): void;
        onBeforeUpdate(callback: (item: any) => q.Promise<any>): void;
        onAfterUpdate(callback: (item: any) => q.Promise<any>): void;
        onBeforeDelete(callback: (item: any) => q.Promise<any>): void;
        onAfterDelete(callback: (item: any) => q.Promise<any>): void;
        insert(itemToCreate: any): q.Promise<any>;
        insertAndSelect(itemToCreate: any): q.Promise<any>;
        update(itemToUpdate: any): q.Promise<any>;
        updateAndSelect(itemToUpdate: any): q.Promise<any>;
        updateItems(valuesToUpdate: any, where: any): q.Promise<any>;
        updateOrInsert(item: any): q.Promise<any>;
        updateOrInsertAndSelect(item: any): q.Promise<any>;
        delete(itemToDelete: any): q.Promise<any>;
        selectById(id: any): q.Promise<any>;
        select(selectOptions: dl.ormso.ISelectOptions): q.Promise<any[]>;
        appendFixedWhere(where: any): void;
        getColumn(columnName: string): dl.ormso.IColumn;
        private createCustomSelectOptions(selectOptions);
        private getBaseTables();
        private getCombinedWhere(customWhere);
        private expand(expand, rows);
        private saveChildRelations(row);
        private executeTrigger(itemToChange, eventVariable);
    }
}