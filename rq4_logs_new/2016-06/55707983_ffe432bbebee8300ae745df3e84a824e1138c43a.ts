import { BinaryDOMOperator } from './BinaryDOMOperator';

export class ActionOperator extends BinaryDOMOperator {
    /**
     *
     */
    constructor() {
        super();
        this.leftOperator = "<<";
        this.rightOperator = ">>";
    }
    
    public applyOperatorValues(valueBindingsPaths : Array<string>, valueBindingsValues : Array<string>, html : string) : string {
        valueBindingsPaths.forEach((bindingPath : string, index) => {
            html = html.replace(this.leftOperator+bindingPath+this.rightOperator, valueBindingsValues[index]);
        });
        return html;
    }
}