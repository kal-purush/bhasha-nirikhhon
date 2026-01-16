import React from 'react'
import { makeStyles } from '@material-ui/core/styles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';

const useStyles = makeStyles({
    table: {
      minWidth: 650,
    },
  })

export default function Display(props) {
    const classes = useStyles();


    const districts = Object.keys(props.districtTable || {})
        .map((el) => ({
            districtName: el,
            confirmed: props.districtTable[el].confirmed,
        }))


    return (
        <div>
        <TableContainer component={Paper}>
            <Table className={classes.table} aria-label="simple table">
                <TableHead>
                <TableRow>
                    <TableCell>DISTRICT NAME</TableCell>
                    <TableCell align="right">CONFIRMED</TableCell>
                </TableRow>
                </TableHead>
                <TableBody>
                
                    {/* <TableRow >
                    <TableCell component="th" scope="row">
                        {props.districtName}
                    </TableCell>
                    <TableCell align="right">{props.confirmed}</TableCell>
                    </TableRow> */}

                    {districts.map((dist) => (
                        <TableRow >
                            <TableCell component="th" scope="row">
                                <b>{dist.districtName}</b>
                            </TableCell>
                            <TableCell align="right"><b>{dist.confirmed}</b></TableCell>
                        </TableRow>
                    ))}


                </TableBody>
            </Table>
            </TableContainer>
        </div>
    )
}