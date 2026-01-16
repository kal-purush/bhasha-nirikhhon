import { Component }       	from '@angular/core';
import { AgGridNg2 } from 'ag-grid-ng2/main';
import {GridOptions} from 'ag-grid/main';

@Component ({
	selector: 
		'events-grid',
	templateUrl: 
		'./app/eventsGrid.component.html',
	directives: [AgGridNg2],
})


export class EventsGridComponent  { 
 // todo   Sort numbers correctly without leading zeros
 //todo  tooltip additional informaion, like sediment family, sample medium, original submitted date/time, etc

private gridOptions: GridOptions;
    private showGrid: boolean;
    private rowData: any[];
    private columnDefs: any[];
    private rowCount: string;
 

	



	
	constructor() {
        // we pass an empty gridOptions in, so we can grab the api out
        this.gridOptions = <GridOptions>{};
		this.createRowData();
        this.createColumnDefs();
        this.showGrid = true;
    }
	
	private createRowData() {
	this.rowData = [
        {"eventDefaultDate":"2016-05-18","stationID":"123456","stationName":"Jordan","totalNumContainers":"10"},
		{"eventDefaultDate":"2015-05-07","stationID":"234234","stationName":"Pideon River at Crossroads","totalNumContainers":"20"},
		{"eventDefaultDate":"2015-05-01","stationID":"103283","stationName":"Pipestone creek","totalNumContainers":"40"},
		{"eventDefaultDate":"2016-04-14","stationID":"345344","stationName":"St. Croix","totalNumContainers":"12"},
		{"eventDefaultDate":"2016-02-12","stationID":"789787","stationName":"Scanlon","totalNumContainers":"8"},
		{"eventDefaultDate":"2016-01-28","stationID":"098889","stationName":"St. Paul Science Museum","totalNumContainers":"10"},
		{"eventDefaultDate":"2015-11-11","stationID":"787909","stationName":"Mississippi at Brainerd","totalNumContainers":"4"},
		{"eventDefaultDate":"2014-03-03","stationID":"980899","stationName":"Harmony crossing","totalNumContainers":"16"},
		        {"eventDefaultDate":"2016-05-18","stationID":"123456","stationName":"Jordan","totalNumContainers":"10"},
		{"eventDefaultDate":"2015-05-07","stationID":"234234","stationName":"Pideon River at Crossroads","totalNumContainers":"20"},
		{"eventDefaultDate":"2015-05-01","stationID":"103283","stationName":"Pipestone creek","totalNumContainers":"40"},
		{"eventDefaultDate":"2016-04-14","stationID":"345344","stationName":"St. Croix","totalNumContainers":"12"},
		{"eventDefaultDate":"2016-02-12","stationID":"789787","stationName":"Scanlon","totalNumContainers":"8"},
		{"eventDefaultDate":"2016-01-28","stationID":"098889","stationName":"St. Paul Science Museum","totalNumContainers":"10"},
		{"eventDefaultDate":"2015-11-11","stationID":"787909","stationName":"Mississippi at Brainerd","totalNumContainers":"4"},
		{"eventDefaultDate":"2014-03-03","stationID":"980899","stationName":"Harmony crossing","totalNumContainers":"16"},
		        {"eventDefaultDate":"2016-05-18","stationID":"123456","stationName":"Jordan","totalNumContainers":"10"},
		{"eventDefaultDate":"2015-05-07","stationID":"234234","stationName":"Pideon River at Crossroads","totalNumContainers":"20"},
		{"eventDefaultDate":"2015-05-01","stationID":"103283","stationName":"Pipestone creek","totalNumContainers":"40"},
		{"eventDefaultDate":"2016-04-14","stationID":"345344","stationName":"St. Croix","totalNumContainers":"12"},
		{"eventDefaultDate":"2016-02-12","stationID":"789787","stationName":"Scanlon","totalNumContainers":"8"},
		{"eventDefaultDate":"2016-01-28","stationID":"098889","stationName":"St. Paul Science Museum","totalNumContainers":"10"},
		{"eventDefaultDate":"2015-11-11","stationID":"787909","stationName":"Mississippi at Brainerd","totalNumContainers":"4"},
		{"eventDefaultDate":"2014-03-03","stationID":"980899","stationName":"Harmony crossing","totalNumContainers":"16"},
		{"eventDefaultDate":"2016-03-13","stationID":"354345","stationName":"St. Louis","totalNumContainers":"15"}
    ];
	}


	private createColumnDefs() {
	
	
	this.columnDefs = [
        {headerName: 'Date', field: "eventDefaultDate", width: 200 },
        {headerName: 'Station ID', field: "stationID" ,width:180},
        {headerName: 'Station Name', field: "stationName" ,width:160},
		{headerName: 'Total # of Containers', field: "totalNumContainers" ,width:160}
    ];
	}
	
	//gridOptions
	
//	temp = this.gridOptions.setRowData(myRowDataToo);

private calculateRowCount() {
        if (this.gridOptions.api && this.rowData) {
            var model = this.gridOptions.api.getModel();
            var totalRows = this.rowData.length;
            var processedRows = model.getRowCount();
            this.rowCount = processedRows.toLocaleString() + ' / ' + totalRows.toLocaleString();
        }
    }

    private onModelUpdated() {
  //      console.log('onModelUpdated');
        this.calculateRowCount();
    }

    private onReady() {
    //    console.log('onReady');
        this.calculateRowCount();
    }

    private onCellClicked($event) {
    //    console.log('onCellClicked: ' + $event.rowIndex + ' ' + $event.colDef.field);
    }

    private onCellValueChanged($event) {
   //     console.log('onCellValueChanged: ' + $event.oldValue + ' to ' + $event.newValue);
    }

    private onCellDoubleClicked($event) {
   //     console.log('onCellDoubleClicked: ' + $event.rowIndex + ' ' + $event.colDef.field);
    }

    private onCellContextMenu($event) {
    //    console.log('onCellContextMenu: ' + $event.rowIndex + ' ' + $event.colDef.field);
    }

    private onCellFocused($event) {
   //     console.log('onCellFocused: (' + $event.rowIndex + ',' + $event.colIndex + ')');
    }

    private onRowSelected($event) {
       // console.log('onRowSelected: ' + $event.node.data.name);
		console.log('ROWS SELECTED: ' );
		var nodes:any[] = this.gridOptions.api.getSelectedNodes();
		console.log(nodes);
		
		//for(i=0;)
    }

    private onSelectionChanged() {
    //    console.log('selectionChanged');
    }

    private onBeforeFilterChanged() {
   //     console.log('beforeFilterChanged');
    }

    private onAfterFilterChanged() {
   //     console.log('afterFilterChanged');
    }

    private onFilterModified() {
   //     console.log('onFilterModified');
    }

    private onBeforeSortChanged() {
   //     console.log('onBeforeSortChanged');
    }

    private onAfterSortChanged() {
   //     console.log('onAfterSortChanged');
    }

    private onVirtualRowRemoved($event) {
        // because this event gets fired LOTS of times, we don't print it to the
        // console. if you want to see it, just uncomment out this line
        // console.log('onVirtualRowRemoved: ' + $event.rowIndex);
    }

    private onRowClicked($event) {
        console.log('onRowClicked: ' + $event.node.data.name);
    }

    private onQuickFilterChanged($event) {
     //   this.gridOptions.api.setQuickFilter($event.target.value);
    }

    // here we use one generic event to handle all the column type events.
    // the method just prints the event name
    private onColumnEvent($event) {
        console.log('onColumnEvent: ' + $event);
    }




}