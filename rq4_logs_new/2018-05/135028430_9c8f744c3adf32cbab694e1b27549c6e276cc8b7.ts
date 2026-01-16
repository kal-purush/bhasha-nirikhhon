import { SortingOptions } from "../options/SortingOptions";
//DEPRECATED
export class AbstractSorter {
    private readonly rows: JQuery<Element>;
    private readonly table: JQuery<Element>;
    private options: SortingOptions;
    public constructor(options: SortingOptions, table: JQuery<Element>) {
        this.options = options;
        this.table = table;
        this.rows = this.getRows();
    }

    private getRows(): JQuery<Element> {
        return this.table.find(this.options.getRowSelector());
    }
}