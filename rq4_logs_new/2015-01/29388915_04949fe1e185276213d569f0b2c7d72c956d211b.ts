
module JsGrid {
    
//Adding Elements to an existing Table:
//***************************************************************************************************************


//Adding new Rows to an existing Table:
    export type RowDefinitionDetails = {
        rowId?: string;
        content?: { [key: string]: CellDefinition; };           //{ columnId: CellDefinition, ...}
    };
    export type RowDefinition = string | Row | RowDefinitionDetails;
    
    export enum RowType { title = 0, body = 1 };

//Adding new Columns to an existing Table:
    export type ColumnDefinitionDetails = {
        columnId?: string;      //Internal representation
        //sortable?: boolean;
        //...
        
        //The following information is needed to create cells when a new column is inserted (to define the content for each new cell):
        content?: { [key: string]: CellDefinition; };   //key == rowId;   { "rowId": CellDefinition};   If a specific row is not defined, the defaultContent is used
        
        defaultTitleContent?: CellDefinition;
        defaultBodyContent?: CellDefinition;  
        //defaultFooterContent?: CellDefinition;   
    };
    export type ColumnDefinition = string | Column | ColumnDefinitionDetails;

//Cells:
    export type CellDefinitionDetails = {
        content: string;
    };
    export type CellDefinition = string | Cell | CellDefinitionDetails;
    




//Serialising & Deserialising:
//***************************************************************************************************************
    export type CellDescription = CellDefinition;

    export type ColumnDescription = {
        columnId: string;       //Internal representation
        defaultTitleContent?:   CellDescription;
        defaultBodyContent?:    CellDescription;  
    };

    export type RowDescription = {
        rowId:    string;
        content?: {             //Only available if the data has been serialised WITH the data, and not only metadata
            [key: string]: CellDescription;      //{ columnId: CellDescription, ...}
        };          
    };

    export type TableDescription = {
        columns:        ColumnDescription[];
        rows:           RowDescription[];
        titleRowCount?: number;             //Default: 0
    };















} 