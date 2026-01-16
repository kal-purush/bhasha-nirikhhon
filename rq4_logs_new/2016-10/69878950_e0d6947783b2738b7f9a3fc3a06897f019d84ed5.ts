import {NgModule} from "@angular/core";
import {BootstrapPaginator} from "./BootstrapPaginator";
import {DataTable} from "./DataTable";
import {DefaultSorter} from "./DefaultSorter";
import {Paginator} from "./Paginator";
import {RowSelector} from "./RowSelector";
import {RowSelectorHead} from "./RowSelectorHead";
import {CommonModule} from "@angular/common";

@NgModule({
    imports: [CommonModule],
    declarations: [BootstrapPaginator, DataTable, DefaultSorter, Paginator, RowSelector, RowSelectorHead],
    exports: [BootstrapPaginator, DataTable, DefaultSorter, Paginator, RowSelector, RowSelectorHead]
})
export class DataTableModule {
}