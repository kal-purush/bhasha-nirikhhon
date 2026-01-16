/**
 * Created by jcabresos on 4/11/15.
 */
import kola = require('kola');
import signals = require('kola-signals');
import hooks = require('kola-hooks');
import fancifier = require('./fancifier/app');
import Editor = require('./views/Editor');

export interface Kontext extends kola.Kontext {

}


export class App extends kola.App<HTMLElement> {

    currentlyTyping: string[];
    editor: Editor;

    fancifierApp: fancifier.App;

    codeAppend: signals.Dispatcher<string[]>;

    initialize(kontext: Kontext): void {
        kontext.setSignal<string[]>('code.append');
        kontext.setSignal<{input: string[]; output: string[]}>('code.hint');
    }

    onStart(): void {
        this.editor = new Editor();
        this.editor.appendTo(this.opts);

        this.fancifierApp = new fancifier.App(this);
        this.fancifierApp.start(this.editor.output);

        this.codeAppend = this.kontext.getSignal('code.append');

        this.editor.input.addEventListener('keypress', this.keyDownHandler.bind(this));
    }

    keyDownHandler(e: KeyboardEvent): void {
        if(!this.currentlyTyping)
            this.currentlyTyping = [];

        if(e.ctrlKey && e.keyCode == 32)
            return this.kontext.getSignal('code.hint').dispatch({input: this.currentlyTyping});

        if(e.keyCode == 32 || e.keyCode == 13) {
            this.codeAppend.dispatch(this.currentlyTyping.splice(0));
            this.currentlyTyping = [];

            this.codeAppend.dispatch([String.fromCharCode(e.charCode)]);

            return;
        }

        this.currentlyTyping.push(String.fromCharCode(e.charCode));
    }

}