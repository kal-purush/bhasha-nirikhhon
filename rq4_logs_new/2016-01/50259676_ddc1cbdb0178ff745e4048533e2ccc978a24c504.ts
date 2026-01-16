declare module Blockly {
    class Workspace {
    }

    class Xml {
        domToWorkspace(workspace:Blockly.Workspace,dom:XMLDocument)
    }

    export default {
        Xml: Blockly.Xml,
        inject: (container:(Element|string), opt_options?) => Blockly.Workspace
    }
}

import {ElementRef, Component, EventEmitter, Output, AfterViewInit} from 'angular2/core';
import Blockly from 'blockly';

Blockly.Blocks['program_base'] = {
    init: function () {
        this.appendDummyInput()
            .appendField("Program started");
        this.setInputsInline(true);
        this.setNextStatement(true);
        this.setColour(60);
        this.setTooltip('');
        this.setHelpUrl('http://www.example.com/');
    }
};

Blockly.Blocks['repeat'] = {
    init: function () {
        this.appendDummyInput()
            .appendField("repeat")
            .appendField(new Blockly.FieldDropdown([["2", "2"], ["3", "3"], ["4", "4"], ["5", "5"], ["6", "6"], ["7", "7"], ["8", "8"], ["9", "9"], ["10", "10"]]), "n")
            .appendField("times");
        this.appendStatementInput("body");
        this.setInputsInline(true);
        this.setPreviousStatement(true);
        this.setNextStatement(true);
        this.setColour(290);
        this.setTooltip('');
        this.setHelpUrl('http://www.example.com/');
    }
};

Blockly.Blocks['move_forward'] = {
    init: function () {
        this.appendDummyInput()
            .appendField("Move forward");
        this.setInputsInline(true);
        this.setPreviousStatement(true);
        this.setNextStatement(true);
        this.setColour(120);
        this.setTooltip('');
        this.setHelpUrl('http://www.example.com/');
    }
};

@Component({
    selector: 'blockly-area',
    inputs: ['content'],
    outputs: ['contentChange: change'],
    template: `
<div class="editor"></div>
<xml class="toolbox" style="display: none">
  <block type="move_forward"></block>
  <block type="repeat"></block>
</xml>
`
})
export class BlocklyArea implements AfterViewInit {
    private blockly;
    private element:HTMLElement;

    @Output() contentChange:EventEmitter<string> = new EventEmitter();

    constructor(elementRef:ElementRef) {
        this.element = <HTMLElement>elementRef.nativeElement;
    }

    ngAfterViewInit():any {
        this.blockly = Blockly.inject(this.element.querySelector('.editor'), {
            toolbox: this.element.querySelector('.toolbox'),
            scrollbars: true
        });
        var xml = '<xml><block type="program_base" deletable="false" movable="false"></block></xml>';
        Blockly.Xml.domToWorkspace(this.blockly, Blockly.Xml.textToDom(xml));
    }
}