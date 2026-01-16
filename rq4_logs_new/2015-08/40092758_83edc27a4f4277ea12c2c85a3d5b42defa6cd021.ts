/// <reference path="../_all.ts" />
module app.todo {
        export interface IToDoItemDirectiveScope extends ng.IScope {
			item: IToDo;
		}
}