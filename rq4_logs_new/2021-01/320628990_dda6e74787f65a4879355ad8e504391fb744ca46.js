import React, { useState, useEffect } from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import IconButton from '@material-ui/core/IconButton';
import ClearIcon from '@material-ui/icons/Clear';
import useStore from '../store';

const columns = [
  { field: 'Title', headerName: 'Apartment', width: 150 },
  { field: 'Price', headerName: 'Price', width: 150 },
  { field: 'From', headerName: 'From', width: 150 },
  { field: 'To', headerName: 'To', width: 150 },
  { field: '', headerName: '', width: 150 },
];

const styles = {
  table: {
    maxWidth: '600px',
  },
  columns: {
    color: '#49a6e9',
    fontSize: '18px',
    fontWeight: 600,
  },
  rows: {
    fontSize: '16px',
    fontWeight: 600,
    color: '#077856',
  },
};

const OrderTable = () => {
  const [changed, setChanged] = useState(false);

  const basketItems = useStore((state) => state.basketItems);
  const removeItemFromBasket = useStore((state) => state.removeItemFromBasket);

  const tableColumns = columns.map((col, index) => (
    <TableCell key={index} align="center" style={styles.columns}>
      {col.headerName}
    </TableCell>
  ));

  const removeRow = (row) => {
    removeItemFromBasket(row);
    setChanged(!changed);
  };

  useEffect(() => {}, [changed]);

  const Rows = basketItems.map((row, index) => (
    <TableRow key={index}>
      <TableCell style={styles.rows} align="center">
        {row.Title}
      </TableCell>
      <TableCell style={styles.rows} align="center">
        €{row.Price}
      </TableCell>
      <TableCell style={styles.rows} align="center">
        {row.From}
      </TableCell>
      <TableCell style={styles.rows} align="center">
        {row.To}
      </TableCell>
      <TableCell key={index} align="center">
        <IconButton
          className="clearBtn"
          key={row.id}
          onClick={() => removeRow(row)}
        >
          <ClearIcon />
        </IconButton>
      </TableCell>
    </TableRow>
  ));

  return (
    <>
      <Table style={styles.table} className="table" aria-label="simple table">
        <TableHead>
          <TableRow>{tableColumns}</TableRow>
        </TableHead>
        <TableBody>{Rows}</TableBody>
      </Table>
    </>
  );
};

export default OrderTable;