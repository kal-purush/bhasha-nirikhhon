import type { Column } from '@tanstack/react-table';

type DataTableColumnMeta = {
  headerClassName?: string;
};

// A factory function to create a getter for column meta
function makeColumnMetaGetter<Meta extends object>() {
  return (column: Column<any, unknown>) => column.columnDef.meta as Meta;
}

class DataTableMetaHelper {
  public static makeColumnMeta(meta: DataTableColumnMeta) {
    return meta;
  }

  public static getColumnMeta = makeColumnMetaGetter<DataTableColumnMeta>();
}

export { DataTableMetaHelper };