import {Component} from 'angular2/core';
import {AceEditor} from './ace';

@Component({
    selector: 'editor',
    template: `
<div class="ui grid">
    <div class="row">
        <div class="four wide column">
            <div class="ui segment">
                <div class="ui list">
                    <div class="item">
                        <i class="folder icon"></i>
                        <div class="content">
                            <div class="header">public</div>
                            <div class="description">Your public EduCraft modules</div>
                            <div class="list">
                                <div class="item">
                                    <i class="file icon"></i>
                                    <div class="content">
                                        <div class="header">awesome-module</div>
                                        <div class="description">Updated 21 mins ago</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <div class="twelve wide column">
            <div class="ui segment">
                <div ace-editor></div>
            </div>
        </div>
    </div>
</div>
`,
    directives: [AceEditor],
})
export class Editor {
}