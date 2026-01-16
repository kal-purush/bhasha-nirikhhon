///<reference path="../typings/references.d.ts" />
import m = require('mithril');
import findIndex = require('lodash/array/findIndex');
import forOwn = require('lodash/object/forOwn');
// Example Data:

var exampleData: Array<IServerData> = [
	{id: 0, value: 'one'},
	{id: 1, value: 'two'},
	{id: 2, value: 'three'},
]

// imported from ../stores/blah
var appStore: GriddzStore = {
	_data: [],
	
	getData: function (url: string) {
		// some ajax here
		return exampleData
	},
	
	loadData: function(data:IServerData[]) {
		for(var i=0, l=data.length; i<l; i++) {
			this._data.push({
				id: m.prop(data[i].id),
				value: m.prop(data[i].value)
			})
		}	
	},
	
	updateCell: function(id:string|number, updates: IUpdateData) {
		var get = findIndex(this._data, {'id': id});
		var config, className;
		forOwn(updates, function(key) {
			config = (key === 'config') ? updates.config : {};
			className = (key === 'className') ? updates.className : '';
		}) 
		this._data[get] = {
			value: m.prop(updates.value),
			config: m.prop(config),
			className: m.prop(className)
		}
	}
}


var viewModelMap = function(signature) {
    var map = {}
    return function(key) {
        if (!map[key]) {
            map[key] = {}
            for (var prop in signature) map[key][prop] = m.prop(signature[prop]())
        }
        return map[key]
    }
}

function focusElement(element:HTMLElement, isInitialized:boolean, context?:Object, vdom?:MithrilVirtualElement) {
	if (isInitialized) return;
	element.focus();
}

var app = {
	// Store:
	store: appStore,
	//etc...
	
	// Controller:
	// calls the functions and sets them where they need to be similar to actions and store in React
	controller: function() {
		this.tempData = app.store.getData('')
		this.setData = () => {
			app.store.loadData(this.tempData); 
			this.tempData = null;
		}
		this.setData(this.tempData);
	},
	
	// View Model/State UI:
	// calls additional function for State UI which is similar to handle events/setState in React
	vm: viewModelMap({
		isEditing: m.prop(false),
		tempValue: m.prop(''),
		error: m.prop('')
	}),
	
	view: function(ctrl?: Function): MithrilVirtualElement {
		return m('div', 'Hello App!', [
			app.store._data.map(function(data, index) {
				var _vm = app.vm(data.id())
				return m('div', 
					_vm.isEditing() ? m('input', {
						oninput: m.withAttr('value', _vm.tempValue), 
						value: _vm.tempValue() ? _vm.tempValue() : data.value(), 
						onblur: function() { 
							data.value(this.value);
							_vm.isEditing(!_vm.isEditing());
							_vm.tempValue('');
						}, config: focusElement
					}) : m('div', {
						onclick: function(){
							_vm.isEditing(!_vm.isEditing())
						}
					}, data.value())
				)
			})
		])
	}
}

//if (this.vm.isEditor) {
//	return m('div', {'id': data.id(), onclick: ctrl.vm.toggle() }, data.value())
//} else {
//	return m('input', {onblur: ctrl.vm.toggle()}, ctrl.vm)
//}

m.mount(document.getElementById('app'), app)

interface GriddzStore {
	_data: IProps[];
	getData(url: string): void; // [R]ead
	loadData(data: IServerData[]): void; // [R]ead
	updateCell(id:string|number, updates: IUpdateData): void; //[U]pdate
}

interface GriddzController {
}

// Component-specific Interfaces
interface IProps {
	id: (s?:string|number) => string | number;
	value: (s?:string) => string;
	config?: (c?:IAppConfig) => IAppConfig;
	className?: (s?:string) => string;
}

interface IServerData {
	id: string | number;
	value: string;
	config?: IAppConfig;
	className?: string;
}

interface IUpdateData {
	value?: string;
	config?: IAppConfig;
	className?: string;
}

interface IAppConfig {
}