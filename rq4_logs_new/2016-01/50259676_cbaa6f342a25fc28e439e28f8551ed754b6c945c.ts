import {Component, EventEmitter, Output, Input, OnInit} from 'angular2/core';
import {AceEditor} from './ace';
import {UserService} from "./user.service";
import {Module} from "./module";
import {BlocklyArea} from "./blockly/blockly";

@Component({
    selector: 'blockly-editor',
    inputs: ['workspace'],
    template: `
<div class="ui piled segments">
    <div class="ui clearing segment">
        <div class="ui fluid labeled action input">
            <div class="ui basic label">
                {{user.username}}/
            </div>
            <input type="text" placeholder="module-name" [(ngModel)]="name">
            <div class="ui icon button" (click)="save()" [ngClass]="{disabled: !modified}">
                <i class="ui save icon"></i>
            </div>
        </div>
    </div>
    <div class="ui segment">
        <div class="ui tabular menu">
            <div class="item" data-tab="tab-name">Blockly</div>
            <div class="item" data-tab="tab-name2">Code</div>
        </div>
        <div class="ui tab" data-tab="tab-name">
            <blockly-area (codeChange)="code=$event"></blockly-area>
        </div>
        <div class="ui tab" data-tab="tab-name2">
            <div ace-editor [content]="code"></div>
        </div>
    </div>
</div>
`,
    directives: [AceEditor, BlocklyArea],
})
export class BlocklyEditor implements OnInit {
    public user = {};
    private initialName:string;
    private initialWorkspace:string;
    private name:string;
    private code:string = '';
    @Output() moduleSaved:EventEmitter<Module> = new EventEmitter();

    constructor(private _userService:UserService) {
        _userService.getUser().subscribe(user => {
            this.user = user;
        });
    }

    ngOnInit() {
        $('.tabular.menu .item').tab();
    }

    save() {
        if (/[0-9A-Za-z\-]+/.test(this.name)) {
            this.moduleSaved.emit({name: this.name, code: this.code});
            this.initialName = this.name;
            this.initialWorkspace = this.code;
        } else {
            alert('Only alphanumeric characters are allowed for the module name.');
        }
    }

    set module(module:Module) {
        this.initialName = module != null ? module.name : '';
        this.initialWorkspace = module != null ? module.code : '';
        this.name = this.initialName;
        this.code = this.initialWorkspace;
    }

    get modified() {
        return this.code != this.initialWorkspace || this.name != this.initialName;
    }
}