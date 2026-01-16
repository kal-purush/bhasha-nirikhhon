import ReactDataGrid from 'react-data-grid';
 
const columns = [{ key: 'id', name: 'ID' }, { key: 'Name', name: 'Name' }, { key: 'Email', name: 'Email'}];
const rows = [{ id: 1, title: 'Title 1' }, ...];
const rowGetter = rowNumber => rows[rowNumber];
 
const Grid = () => {
  return <ReactDataGrid
    columns={columns}
    rowGetter={rowGetter}
    rowsCount={rows.length}
    minHeight={500} />);
}