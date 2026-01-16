/// <reference path="../typings/tsd.d.ts" />
import {ElementRef, Directive, EventEmitter} from 'angular2/core';

@Directive({
    selector: '[ace-editor]',
    inputs: ['content'],
    outputs: ['contentChange: change']
})
export class AceEditor {
    // http://ace.c9.io/#nav=api&api=editor
    editor;

    /** When the markdown content changes we broadcast the entire document. */
    contentChange:EventEmitter<string> = new EventEmitter();

    constructor(elementRef:ElementRef) {
        // Note the constructor doesn't have access to any data from properties
        // We can instead use a setter

        // This is the <div ace-editor> root element
        // Ideally this wouldn't be required
        var el = elementRef.nativeElement;

        el.classList.add("editor");
        el.style.height = "250px";
        el.style.width = "100%";

        this.editor = ace.edit(el);
        this.editor.setTheme("ace/theme/chrome");
        this.editor.getSession().setMode("ace/mode/lua");

        //this.editor.$blockScrolling = Infinity;

        this.editor.on("change", (e) => {
            // Discard the delta (e), and provide whole document
            this.contentChange.emit(this.editor.getValue());
        });

    }

    set content(text) {
        this.editor.setValue(text);
        this.editor.clearSelection();
        this.editor.focus();
    }
}