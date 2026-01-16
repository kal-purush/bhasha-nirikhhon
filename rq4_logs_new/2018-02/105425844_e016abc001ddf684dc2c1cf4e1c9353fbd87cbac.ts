//add "AceEditorDirective" to your modules list 

//import { AceEditorDirective } from 'ng2-ace-editor'; 
import { Component } from '@angular/core';

//to use theme "eclipse" 
//with angular-cli add "../node_modules/ace-builds/src-min/ace.js"  
//and "../node_modules/ace-builds/src-min/theme-eclipse.js" to "scripts" var into the file angular-cli.json 

@Component({
    template: `
  <div ace-editor
       [(text)]="text" 
       [mode]="'json'" 
       [options]="options"
       [readOnly]="false"
       [autoUpdateContent]="true" //change content when [text] change
       [durationBeforeCallback]="1000" //wait 1s before callback 'textChanged' sends new value
       (textChanged)="onChange($event)"
       style="min-height: 200px; width:100%; overflow: auto;"></div>
  `
})
export class AceEditor {
    text: string = "";
    options: any = { maxLines: 1000, printMargin: false };

    onChange(code: any) {
        console.log("new code", code);
    }
}