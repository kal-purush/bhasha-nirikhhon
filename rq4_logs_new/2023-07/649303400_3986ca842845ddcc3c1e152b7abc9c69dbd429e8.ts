import { ITableHeader } from 'components/Table'
import Cell from 'components/Table/components/ExpandableCell'

export function getHeaderList(): ITableHeader[] {
  const headers: ITableHeader[] = [
    {
      Header: '',
      accessor: 'isExpand',
      Cell
    },
    {
      Header: 'COVER',
      accessor: 'image',
      disableSortBy: true
    },
    {
      Header: 'TITLE',
      accessor: 'title',
      Cell
    },
    {
      Header: 'AUTHOR',
      accessor: 'author',
      Cell
    },
    {
      Header: 'PRICE',
      accessor: 'price',
      Cell
    },
    {
      Header: '',
      accessor: 'actions'
    }
  ]
  return headers
}