///<reference path="../../typings/references.d.ts" />
import m = require('mithril');
import Cell = require('./cell');

var cell = new Cell();

class Row implements MithrilComponent {
	
	controller:any = function(): any {
		this.buildFields = function(data:IServerData): any {
			var isStatic = data.config['isStatic']
			return m.component(cell, {
				value: data.value, 
				isStatic: isStatic
			})
		}
	}
	
	view(ctrl?: any, args?: any): MithrilVirtualElement {
		var cells = [];
		Object.keys(args.data).forEach(function(prop:string){
			if (prop != 'config') {
				cells.push(ctrl.buildFields({value: args.data[prop], config: args.data['config']}))
			}
		});
		return m('tr', cells);
	}
}

export = Row;