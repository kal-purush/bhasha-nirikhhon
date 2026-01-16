/// <reference path="../typings/lodash/lodash.d.ts" />
import _ = require('lodash');

export interface IUndoRedoCommand{
    undo():void;
	redo():void;
}

export class PropertyChangeCommand implements IUndoRedoCommand{
	private _propertyName:string;
	private _oldValue:any;
	private _newValue:any;
	private _target:any;
	
	constructor(target:any, propertyName:string, newValue:any, oldValue:any){
		this._target = target;
		this._propertyName = propertyName;
		this._newValue = newValue;
		this._oldValue = oldValue;
	}
	
	public get oldValue():any{
		return this._oldValue;
	}
	
	public get newValue():any{
		return this._newValue;
	}
	
	public get target():any{
		return this._target;
	}
	
	public get propertyName():string{
		return this._propertyName;
	}
	
	public undo():void{
		this._target[this._propertyName] = this._oldValue;
	}
	
	public redo():void{
		this._target[this._propertyName] = this._newValue;
	}
	
}

export class CompositeCommand implements IUndoRedoCommand{
	private _commands:Array<IUndoRedoCommand>;
	
	public get commands():Array<IUndoRedoCommand>{
		return this._commands;
	}
	
	constructor(commands:Array<IUndoRedoCommand>){
		this._commands = commands.concat();
	}
	
	public undo():void{
		_.forEach(this._commands, (command:IUndoRedoCommand)=>{
			command.undo();
		});
	}
	
	public redo():void{
		_.forEach(this._commands, (command:IUndoRedoCommand)=>{
			command.redo();
		});
	}
}
