import { Table } from "../table/Table";
import { Attributable } from "./Attributable";

export class ColumnAttributeRetriever implements Attributable {
    private table: Table;
    private hasHeader: boolean;
    public constructor(table: Table, hasHeader: boolean) {
        this.table = table;
        this.hasHeader = hasHeader;
    }

    public getAttributeFrom(columnIndex: number, rowIndex: number, attribute: string): string {
        const column = this.table.getColumnFromRow(columnIndex, rowIndex);
        let value = column.data(attribute);
        if (this.valueDoesNotExistsAndHasHeaderToGetValueFrom(value)) {
            value = this.getAttributeFromHeader(columnIndex, attribute);
        }
        return !value ? "" : value;
    }

    private getAttributeFromHeader(columnIndex: number, attribute: string): string {
        const headerColumn = this.table.getColumnFromRow(columnIndex, 0);
        return headerColumn.data(attribute);
    }

    private valueDoesNotExistsAndHasHeaderToGetValueFrom(value: string): boolean {
        return !value && this.hasHeader;
    }
}