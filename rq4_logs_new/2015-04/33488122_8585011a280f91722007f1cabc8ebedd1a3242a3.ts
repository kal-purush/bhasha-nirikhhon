/// <reference path="../../scripts/typings/all.d.ts" />

module app.todo {

    export interface IEditScope {
        description: string;
        dueAt: string;
        id: number;
        cancel(): void;
        save(): void;
    }

    export class EditController implements IEditScope {

        $log: ng.ILogService;
        $rootScope: ng.IRootScopeService;
        todo: app.todo.ITodo;
        todoModalService: app.todo.TodoModalService;
        todoService: app.services.ITodoService;

        static $inject = [
            '$rootScope',
            'app.services.TodoService',
            'app.todo.TodoModalService',
            '$log'
        ];
        constructor($rootScope: ng.IRootScopeService,
                    todoService: app.services.ITodoService,
                    todoModalService: app.todo.TodoModalService,
                    $log: ng.ILogService) {
            this.$rootScope = $rootScope;
            this.todoService = todoService;
            this.todoModalService = todoModalService;
            this.$log = $log;
            this.initialize();
        }

        get description(): string {
            return this.todo.description;
        }
        set description(value: string) {
            this.todo.description = value || '';
        }

        get dueAt(): string {
            return this.todo.dueAt;
        }
        set dueAt(value: string) {
            this.todo.dueAt = new Date(value).toISOString();
        }

        get id(): number {
            return this.todo.id;
        }

        cancel(): void {
            this.todoModalService.hide();
        }

        initialize(): void {
            this.todo = new app.todo.Todo();
            this.$rootScope.$on('edit-todo', (event: any, data: any): void => {
                this.$log.debug('edit-todo', data);
                this.todo = data;
            });
        }

        save(): void {
            this.todoService.save(this.todo);
            this.$rootScope.$emit('save-todo', this.todo);
            this.todoModalService.hide();
        }


    }

    angular.module('app.todo').controller('app.todo.EditController', EditController);

}