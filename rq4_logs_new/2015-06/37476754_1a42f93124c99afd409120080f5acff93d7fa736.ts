///<reference path="../../typings/references.d.ts" />
import m = require('mithril');
import Row = require('./row');

var appRow = new Row();

class Table implements MithrilComponent {
	view(ctrl?:Object, args?:Object): MithrilVirtualElement {
		return m('table', 
			 m('tbody', [
				args['data'].map(function(data:IProps, index:number){
					return m.component(appRow, {
						data: data,
						uid: 'row_' + (index).toString(),
						rowIndex: index,
						active: args['active']
					})
				})
			])
		)
	}
}

export = Table;