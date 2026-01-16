import * as React from 'react';

export type TableRef = HTMLTableElement;
export type TableProps = React.HTMLAttributes<HTMLTableElement>;

export type TableHeaderRef = HTMLTableSectionElement;
export type TableHeaderProps = React.HTMLAttributes<HTMLTableSectionElement>;

export type TableBodyRef = HTMLTableSectionElement;
export type TableBodyProps = React.HTMLAttributes<HTMLTableSectionElement>;

export type TableFooterRef = HTMLTableSectionElement;
export type TableFooterProps = React.HTMLAttributes<HTMLTableSectionElement>;

export type TableRowRef = HTMLTableRowElement;
export type TableRowProps = React.HTMLAttributes<HTMLTableRowElement>;

export type TableHeadRef = HTMLTableCellElement;
export type TableHeadProps = React.ThHTMLAttributes<HTMLTableCellElement>;

export type TableCellRef = HTMLTableCellElement;
export type TableCellProps = React.TdHTMLAttributes<HTMLTableCellElement>;

export type TableCaptionRef = HTMLTableCaptionElement;
export type TableCaptionProps = React.HTMLAttributes<HTMLTableCaptionElement>;

export type TableComponent = React.ForwardRefExoticComponent<
  TableProps & React.RefAttributes<TableRef>
> & {
  Header: React.ForwardRefExoticComponent<
    TableHeaderProps & React.RefAttributes<TableHeaderRef>
  >;
  Body: React.ForwardRefExoticComponent<
    TableBodyProps & React.RefAttributes<TableBodyRef>
  >;
  Footer: React.ForwardRefExoticComponent<
    TableFooterProps & React.RefAttributes<TableFooterRef>
  >;
  Row: React.ForwardRefExoticComponent<
    TableRowProps & React.RefAttributes<TableRowRef>
  >;
  Head: React.ForwardRefExoticComponent<
    TableHeadProps & React.RefAttributes<TableHeadRef>
  >;
  Cell: React.ForwardRefExoticComponent<
    TableCellProps & React.RefAttributes<TableCellRef>
  >;
  Caption: React.ForwardRefExoticComponent<
    TableCaptionProps & React.RefAttributes<TableCaptionRef>
  >;
};