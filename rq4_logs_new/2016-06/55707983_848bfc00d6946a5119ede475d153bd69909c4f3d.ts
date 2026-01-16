export class CKDOMManipulator {
    private static valueBindingLeftOperator : string = "{{";
    private static valueBindingRightOperator : string = "}}";
    private static actionBindingLeftOperator : string = "(";
    private static actionBindingRightOperator : string = ")";

    public static insertNode(anchorSelector : string, html : string) : void {

    }

    public static removeNode(anchorSelector : string, nodeSelector : string) : void {

    }

    public static updateNode(anchorSelector : string, nodeSelector : string, newHtml : string) : void {

    }

    public static extractValueBindings(html : string) : Array<string> {
        let unleftedHtmlArray : Array<string> = html.split(CKDOMManipulator.valueBindingLeftOperator);
        let valueBindings : Array<string> = new Array<string>();

        unleftedHtmlArray.forEach(leftSplitedHtml => {
            let rightSplitedHtmlArray : Array<string> = leftSplitedHtml.split(CKDOMManipulator.valueBindingRightOperator);
            if(rightSplitedHtmlArray.length > 1) {
                valueBindings.push(rightSplitedHtmlArray[0]);
            }
        });

        return valueBindings;
    }

    public static applyValueBindings(valueBindingsPaths : Array<string>, valueBindingsValues : Array<string>, html : string) : string {
        valueBindingsPaths.forEach((bindingPath : string, index) => {
            html = html.replace(CKDOMManipulator.valueBindingLeftOperator+bindingPath+CKDOMManipulator.valueBindingRightOperator, valueBindingsValues[index]);
        });
        return html;
    }

    public static extractActionBindings(html : string) {

    }
}