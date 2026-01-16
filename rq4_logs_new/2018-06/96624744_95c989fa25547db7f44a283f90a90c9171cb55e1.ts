export declare enum DataType {
    Unknown = 0,
    String = 1,
    FixedChar = 2,
    Memo = 3,
    Blob = 4,
    Boolean = 5,
    SmallInt = 6,
    Integer = 7,
    Int64 = 8,
    AutoInc = 9,
    BCD = 10,
    Float = 11,
    DateTime = 12,
    Date = 13,
    Time = 14,
    Guid = 15,
}
export declare class Field {
    fieldName: string;
    dataType: DataType;
    value: Object;
    isNull(): boolean;
    size?: number;
}
export declare class Record {
    tableName: string;
    fields: Field[];
    fieldByName(name: string): Field;
    addField(fieldName: string): Field;
}
export declare class FieldDefinition {
    fieldName: string;
    dataType?: DataType;
    dataTypeStr?: string;
    length?: number;
    precision?: number;
    scale?: number;
    notNull?: boolean;
    autoInc?: boolean;
}
export declare class TableDefinition {
    tableName: string;
    fieldDefs: FieldDefinition[];
    primaryKeys: string[];
}
export declare class CustomMetadataDefinition {
    objectName: string;
    objectType: string;
    SQL: string[];
}
export declare class DatabaseDefinition {
    databaseType: string;
    customMetadata: CustomMetadataDefinition[];
    triggerTemplates: string[];
}