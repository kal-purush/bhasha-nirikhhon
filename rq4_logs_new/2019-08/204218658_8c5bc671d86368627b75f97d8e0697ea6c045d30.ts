import { DataRow } from "./types";

export type ColumnConfig<T extends DataRow> = (ColumnConfigDerived<T> | ColumnConfigField<T>) & ColumnConfigBase<T>;

interface ColumnConfigField<T extends DataRow> {
  /**
   * The field that represents the field to retrieve from the data row.
   */
  fieldSelector: keyof T;
  /**
   * An optional label for the column that will be used instead of the field name.
   */
  label?: string;
}

interface ColumnConfigDerived<T extends DataRow> {
  /**
   * A function to create a derived value for a given row.  Use this for calculated fields.
   */
  fieldSelector: (row: T) => any;
  /**
   * The label for the derived column.  This is required since the field is derived.
   */
  label: string;
}

interface ColumnConfigBase<T extends DataRow> {
  /**
   * Whether this column is filterable.  Defaults to true for primitive types and false for objects/arrays unless
   * filter predicate is provided.
   */
  filterable?: boolean;
  filterOptions?: any; // TODO 'checkbox', 'dropdown', 'multiselect', 'textField'
  filter?: (row: T) => boolean;

  /**
   * Whether the column is sortable or not.  For primitive types this will default to true.  For
   * objects this will default to false, unless sortComparer is provided.
   */
  sortable?: boolean;

  /**
   * The comparer to use to determine sort order for this
   */
  sortComparer?: (first: T, second: T) => number;

  /**
   * Whether this column should be included in search.
   */
  searchable?: boolean;

  /**
   * A function that indicates whether the search criteria matches.
   */
  searchMatcher?: (searchString: string, value: any) => boolean;

  /**
   * Whether the column visibility can be changed.
   */
  canChangeVisibility?: boolean;
}