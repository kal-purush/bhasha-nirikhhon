import React from 'react';
import XLSX from 'xlsx';

// Bootstrap
import { Button , Modal, Table, ButtonGroup} from 'react-bootstrap';
import 'bootstrap/dist/css/bootstrap.min.css';

// Semantic
import { Form } from 'semantic-ui-react'
import 'semantic-ui-css/semantic.min.css'

// moment.js
import * as moment from 'moment';

// Exceljs
import Excel from 'exceljs';

function ExcelPO(props){
    const fileInput = React.useRef(null)
    const [ items , setItems] = React.useState([])
    let [ viewItem , setViewItem ] = React.useState([])
    let [ modalItem , setModalItem ] = React.useState(false)

    let toColumnName = (num) => {
        for (var ret = '', a = 1, b = 26; (num -= a) >= 0; a = b, b *= 26) {
        ret = String.fromCharCode(parseInt((num % b) / a) + 65) + ret;
        }
        return ret;    
    }

    const onDropFile  = (evt) => {
        evt.stopPropagation();
        evt.preventDefault();

        let files = evt.dataTransfer.files;

        handleFileChange(files)
    }

    let handleFileChange = async (files) => {
        let promise =  Array.from(files).map(file => {
            return new Promise((resolve) => {
                let reader = new FileReader()
                reader.onload = (evt) => {
                    const data = evt.target.result
                    let wb = XLSX.read(data, {type: 'binary'})
                    let ws = wb.Sheets[wb.SheetNames[0]] 
                    let json = XLSX.utils.sheet_to_json(ws)
                    console.log(json)
                    const PO_Detail =  {
                        'Purchase Contract': '', 
                        'Date': '',
                        'Issue No.': '', 
                        'Project No.': '',
                        'Contract No.': '',
                    }
                    const itemHeader = ["Line", "Project Area", "Site ID ", "Site Name", "Item Num", "Description", "Remark", "Unit", "Qty", "Unit Price", "Amount"]
                    let nextFill = null
                    let nextItem = null
                    let lineItems = []
                    json.forEach((row) => {
                        Object.entries(row).forEach(([col, colvalue]) => {
                            if(colvalue in PO_Detail){
                                if(PO_Detail[colvalue] === '') nextFill = colvalue 
                            }else if(nextFill !== null){
                                PO_Detail[nextFill] = colvalue 
                                nextFill  = null
                            }
                        })

                        let isAllExist = Object.entries(row).every(([col, colvalue]) => itemHeader.includes(colvalue))
                        if(isAllExist){
                            nextItem = row
                        }else if(nextItem !== null){
                            if(Object.values(row).length < 5){
                                nextItem = null
                            }else{
                                let itemline = Object.entries(nextItem).reduce((obj , [col,colvalue]) => {
                                    if(col in row) obj[colvalue] = row[col]
                                    return obj
                                }, {})
                                lineItems.push(itemline)
                            }
                        }
                    })
                    
                    lineItems.forEach(item => {
                        Object.entries(PO_Detail).forEach(([field, value]) =>{
                            item[field] = value
                        })
                    })
                    let item =  {poDate:PO_Detail.Date, poNumber: PO_Detail['Purchase Contract'], items:lineItems?lineItems:[], filename: file.name}
                    resolve(item)
                    //console.log(text.replace(/(?<w1>\b)\s(?<w2>\b)/g, "$1$2"))
                }
                reader.readAsBinaryString(file)
            })
        })

        Promise.all(promise).then((results) => {
            console.log(results)
            setItems(results)
        })
        
    }

    let exportCSV = () => {
        let content = []
        let totalItems = items.flatMap(item => item.items)
        content = totalItems.map(lineItem => Object.values(lineItem))

        if(content.length > 0){
        let wb = new Excel.Workbook()
        let ws = wb.addWorksheet("PO")
        ws.addRow(Object.keys(totalItems[0]))
        console.log(Object.keys(totalItems[0]))
        console.log(content)

        Object.keys(totalItems[0]).forEach((column,index) => {
            if(column.match(/date/g)){
                console.log(`Convert column ${index+1} to date format`)
                ws.getColumn(`${toColumnName(index+1)}`).numFmt = "d-mmm-yy"
                content.forEach(row => {
                    row[index] = moment.utc(row[index], 'YYYY/MM/DD').toDate()
                })
            }
        })
        ws.addRows(content)

        
        //content = [Object.keys(totalItems[0]).join(","), ...content]
        wb.xlsx.writeBuffer().then((buffer) => {
            let blob = new Blob([buffer],{type:"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"})
            let elem = window.document.createElement("a")
            elem.href = window.URL.createObjectURL(blob)
            elem.download = 'PO_Export.xlsx'
            elem.click()
        })
        }
    }

    return <>
        <div className="dropFileArea" onDragOver={(evt)=>{evt.stopPropagation();evt.preventDefault();evt.dataTransfer.dropEffect = 'copy';}} onDrop={onDropFile}>
            <div style={{display:'flex', height: '40px', verticalAlign:'middle'}}>
            <div style={{padding:'8px'}}>Drop multiple PO Excels files or</div>

            <Button onClick={()=>{
                fileInput.current.click();
            }}>Select Files</Button>

            <input type="file" ref={fileInput} onChange={(evt)=>{
                let files = evt.target.files 
                if(files.length > 0){
                    handleFileChange(files)
                }
            }} style={{display:'none'}} accept=".xlsx, .xls" multiple/>
            </div>
            
            
        </div>
        <div className="actionArea">
            {items.length > 0 && <Button variant="secondary" onClick={()=>{exportCSV()}}>Export to Excel</Button>}
        </div>

        <div className="itemsArea">
            {items.length > 0 && <Table size="sm" hover={true}>
            <thead>
                <tr>
                <td>Filename</td>
                <td>PO Number</td>
                <td>PO Date</td>
                <td>Number of items</td>
                <td></td>
                </tr>
            </thead>
            <tbody>
                {items.map((item,itemIndex) => {
                return <tr key={itemIndex}>
                    <td>{item.filename}</td>
                    <td>{item.poNumber}</td>
                    <td>{item.poDate}</td>
                    <td>{item.items.length}</td>
                    <td>
                    <ButtonGroup size="sm">
                        <Button onClick={()=>{
                            setViewItem(item.items)
                            setModalItem(true)
                        }}>
                        View Item
                        </Button>
                    </ButtonGroup>
                    
                    
                    </td>
                </tr>
                })}
            </tbody>
            </Table>}
        </div>
        <Modal centered scrollable show={modalItem} onHide={()=>{setModalItem(false)}} dialogClassName={'pdf-modal'} size="xl">
            <Modal.Header closeButton>
            <Modal.Title>Items</Modal.Title>
            </Modal.Header>

            <Modal.Body>
            {viewItem.length > 0 && <table className="po-output">
                <thead>
                <tr>
                    {Object.keys(viewItem[0]).map((field, fieldid)=>{
                    return <td key={fieldid}>{`${field}\t`}</td>
                    })}
                </tr>
                </thead>  
                <tbody>
                {viewItem.map((item, itemid)=>{
                    return <tr key={`item_${itemid}`}>
                    {Object.keys(item).map((field, fieldid)=>{
                        return <td key={`item_${itemid}_${fieldid}`}>{`${item[field]}\t`}</td>
                    })}
                    </tr>
                })}
                <tr></tr>
                </tbody>
            </table>}
            </Modal.Body>
        </Modal>
    </>
}

export default ExcelPO