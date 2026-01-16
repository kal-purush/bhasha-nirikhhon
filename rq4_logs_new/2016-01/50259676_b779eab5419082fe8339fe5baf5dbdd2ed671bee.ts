import {Component, EventEmitter, Output, Input} from 'angular2/core';
import {AceEditor} from './ace';
import {UserService} from "./user.service";
import {Module} from "./module";

@Component({
    selector: 'module-editor',
    inputs: ['module'],
    template: `
<div class="ui segments">
    <div class="ui clearing segment">
        <div class="ui fluid labeled action input">
            <div class="ui basic label">
                {{user.username}}/
            </div>
            <input type="text" placeholder="module-name" [(ngModel)]="name">
            <div class="ui icon button" (click)="save()">
                <i class="ui save icon"></i>
            </div>
        </div>
    </div>
    <div class="ui segment">
        <div ace-editor (contentChange)="code=$event" [content]="initialCode"></div>
    </div>
</div>
`,
    directives: [AceEditor],
})
export class ModuleEditor {
    public user = {};
    private name:string;
    private initialCode:string;
    private code:string;
    @Output() moduleSaved:EventEmitter<Module> = new EventEmitter();

    constructor(private _userService:UserService) {
        _userService.getUser().subscribe(user => {
            this.user = user;
        });
    }

    save() {
        if (/[0-9A-Za-z\-]+/.test(this.name)) {
            this.moduleSaved.emit({name: this.name, code: this.code});
        } else {
            alert('Only alphanumeric characters are allowed for the module name.');
        }
    }

    set module(module:Module) {
        this.name = module != null ? module.name : '';
        this.initialCode = module != null ? module.code : '';
        this.code = this.initialCode;
    }
}