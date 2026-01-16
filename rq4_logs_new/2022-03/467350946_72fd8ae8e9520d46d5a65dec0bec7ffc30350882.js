import React from 'react'
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Typography from '@mui/material/Typography';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';

import "../styles.css";

function createData( k, v ) {
    if (k == '字 Han')
        v = String.fromCodePoint(Number('0x' + v));
    return { k, v };
  }

const HanCard = ({ rowdata }) => {
    const cells = [
        createData('字 Han', rowdata[0]),
        createData(<abbr title="Middle Chinese Pinyin">中古 MCP</abbr>, rowdata[1]),
        createData(<abbr title="Putonghua Pinyin">普 manda</abbr>, rowdata[2]),
        createData(<abbr title="Cantonese Jyutping">粤 canto</abbr>, rowdata[3]),
        createData(<abbr title="Shanghainese">吴 sh</abbr>, rowdata[4]),
        createData(<abbr title="Southern Min">闽 mn</abbr>, rowdata[5]),
        createData(<abbr title="Korean">韩 kr</abbr>, rowdata[6]),
        createData(<abbr title="Viernamese">越 vn</abbr>, rowdata[7]),
        createData(<abbr title="Japanese Go-on">日·吴 jp-go</abbr>, rowdata[8]),
        createData(<abbr title="Japanese Kan-on">日·汉 jp-kan</abbr>, rowdata[9])
    ]
    if (rowdata[10])
        cells.push(createData(<abbr title="Japanese Tō-on">日·唐 jp-to</abbr>, rowdata[10]))
    if (rowdata[11])
        cells.push(createData(<abbr title="Japanese Habitual">日·惯 jp-kwan</abbr>, rowdata[11]))    
    if (rowdata[12])
        cells.push(createData(<abbr title="Japanese Other">日·他 jp-other</abbr>, rowdata[12]))

    return (
        <div>
            <Card raised className="card-container">
                <div className="card-content">
                    <CardContent>
                        <Table size="small" aria-label="a dense table">
                            <TableBody>
                                {cells.map(
                                    (row, i) => (
                                        <TableRow
                                            key={i}
                                            // sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
                                        >
                                            <TableCell scope="row" key={i + 'cell1'}>
                                                {row.k}
                                            </TableCell>
                                            <TableCell key={i + 'cell2'} align="left" style={{
                                                    whiteSpace: "normal",
                                                    wordWrap: "break-word"
                                                    }}>{row.v?row.v.replaceAll(',', ', '):''}</TableCell>
                                        </TableRow>
                                    )
                                )}
                            </TableBody>
                        </Table>
                    </CardContent>
                </div>
            </Card>
        </div>
    )
}

export default HanCard;