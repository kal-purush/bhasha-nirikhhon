///<reference path="../../typings/references.d.ts" />
import m = require('mithril');
var _db = require('./mockdb');

var AppStore:GriddzStore = {
	_data: [],
	getData: function(url?:string): void {
		if (url) {
			// TODO: get/return ajax from server...	
		} else {
			this.loadData(_db.assignments);
			 return this._data;
			// console.log(this._data);
			// TODO: set generic config/load config for blank spreadsheet...	
		}
	},
	loadData: function(data: IServerData[]):void {	
		data.map((model:IServerData, rowIndex:number) => {
			var _row = [];
			var colIndex = 0;
			Object.keys(model).forEach((k:string) => {				
				_row.push({
					value: m.prop(model[k]),
					coord: m.prop([colIndex, rowIndex])
				})
				colIndex++;
			})
			this._data.push(_row);
		})
	},
	updateCell: function(id:string|number, updates: IUpdateData):void {
		// TODO: updateCell with new information
	}
}

export = AppStore;