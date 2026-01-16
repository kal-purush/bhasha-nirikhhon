import { Component }       	from '@angular/core';
import { AgGridNg2 } from 'ag-grid-ng2/main';
import {GridOptions} from 'ag-grid/main';

@Component ({
	selector: 
		'event-grid',
	template: 
		'<ag-grid-ng2 #agGrid style="height:100%;width:100%px" class="ag-fresh" [gridOptions]="gridOptions"></ag-grid-ng2 >',
	directives: [AgGridNg2],
})


export class EventGridComponent  { //} implements OnInit{
    constructor() {
        this.gridOptions = {
            rowData: this.myRowData,
            columnDefs: this.columnDefs,
            enableColResize: true,
            enableSorting: true,
            enableFilter: true
        }   
    }

    gridOptions = {};
	
	
 myRowData = [
        {"eventDefaultDate":"2016-05-18","stationID":"123456","stationName":"Jordan","totalNumContainers":"10"},
		{"eventDefaultDate":"2015-05-07","stationID":"234234","stationName":"Pideon River at Crossroads","totalNumContainers":"20"},
		{"eventDefaultDate":"2015-05-01","stationID":"103283","stationName":"Pipestone creek","totalNumContainers":"40"},
		{"eventDefaultDate":"2016-04-14","stationID":"345344","stationName":"St. Croix","totalNumContainers":"12"},
		{"eventDefaultDate":"2016-02-12","stationID":"789787","stationName":"Scanlon","totalNumContainers":"8"},
		{"eventDefaultDate":"2016-01-28","stationID":"098889","stationName":"St. Paul Science Museum","totalNumContainers":"10"},
		{"eventDefaultDate":"2015-11-11","stationID":"787909","stationName":"Mississippi at Brainerd","totalNumContainers":"4"},
		{"eventDefaultDate":"2014-03-03","stationID":"980899","stationName":"Harmony crossing","totalNumContainers":"16"},
		{"eventDefaultDate":"2016-03-13","stationID":"354345","stationName":"St. Louis","totalNumContainers":"15"},

    ];
// todo   Sort numbers correctly without leading zeros

 
    columnDefs = [
        {headerName: 'Date', field: "eventDefaultDate", width: 200 },
        {headerName: 'Station ID', field: "stationID" ,width:180},
        {headerName: 'Station Name', field: "stationName" ,width:160},
		{headerName: 'Total # of Containers', field: "totalNumContainers" ,width:160}
    ];
 
 //todo  tooltip additional informaion, like sediment family, sample medium, original submitted date/time, etc

// todo allow line selection

// todo allow multi-line selection


 





//-------------------



}