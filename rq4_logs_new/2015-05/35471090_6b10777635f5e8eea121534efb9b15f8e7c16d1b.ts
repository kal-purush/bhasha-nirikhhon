interface TextBindingInfo {
    elementBindingId: number;
    textNodeIndex: number;
    bindingPath: string;
    bindingText: string;
}

class Component {
    public static bindingRegex: RegExp = /{{[a-zA-Z._0-9]+}}/g; // Regular expression for bindings
    public static bindingIdCounter: number = 0; // Maintains the next 'unique id' applied to elements tracked for bindings
    public elementId: string;
    public title: Observable<string> = new Observable<string>("");
    protected parentElement: HTMLElement;
    protected htmlElements: Array<HTMLElement> = new Array<HTMLElement>(); // The HTML elements at the root of this component

    protected textBindings: { [bindingPath: string]: Array<TextBindingInfo> } = {}; 

    constructor(id: string, parent?: HTMLElement) {
        this.elementId = id;
        this.parentElement = parent;
        var template: HTMLScriptElement = <HTMLScriptElement>document.getElementById("template_" + this.elementId);
        if (template == null) {
            throw new Error("Tried to instantiate non-existing template");
        }
        var parser = new DOMParser();
        var fragment = parser.parseFromString(template.innerHTML, "text/html");

        for (var i = 0; i < fragment.body.childNodes.length; i++) {
            this.htmlElements.push(<HTMLElement>fragment.body.childNodes.item(i));
        }
        // Process elements for bindings
        for (var i = 0; i < this.htmlElements.length; i++) {
            this.processBindings(this.htmlElements[i]);
        }
        console.log(this.textBindings);
    }

    private processBindings(rootElement: HTMLElement) {
        // Look for bindings in the text nodes
        var textNodes: Array<Node> = [];
        for (var i = 0; i < rootElement.childNodes.length; i++) {
            if (rootElement.childNodes[i].nodeType == 3) { // is a text node
                textNodes.push(rootElement.childNodes[i]);
            }
        }
        
        var bindingId = -1;
        for (var i = 0; i < textNodes.length; i++) {
            var node = textNodes[i];
            var matches = node.nodeValue.match(Component.bindingRegex)
            if (matches.length > 0) {
                if (bindingId == -1) {
                    // generate and apply element binding ID
                    bindingId = Component.bindingIdCounter;
                    Component.bindingIdCounter++;
                    rootElement.id = "BindingElement" + bindingId;
                }
                for (var j = 0; j < matches.length; j++) {
                    var path = matches[j].substr(2, matches[j].length - 4);
                    if (typeof this.textBindings[path] === 'undefined') {
                        this.textBindings[path] = new Array();
                    }
                    this.textBindings[path].push({
                        elementBindingId: bindingId,
                        textNodeIndex: i,
                        bindingPath: path,
                        bindingText: node.nodeValue
                    });
                }
            }
        }

        //for (var matches: Array<{ match: any; index: number; }> = [], match; match = Component.bindingRegex.exec(html);) {
        //    matches.push({ match: match, index: match.slice(1).indexOf(match[0]) });
        //}

        // Process bindings for child elements
        for (var i = 0; i < rootElement.childNodes.length; i++) {
            this.processBindings(<HTMLElement>rootElement.childNodes[i]);
        }
    }

    public show(): void {
        var appendParent = document.body;
        if (this.parentElement != null) {
            appendParent = this.parentElement
        }
        for (var i = 0; i < this.htmlElements.length; i++) {
            this.htmlElements[i] = <HTMLElement>appendParent.appendChild(this.htmlElements[i]);
        }
    }

    public hide(): void {
        for (var i = 0; i < this.htmlElements.length; i++) {
            this.htmlElements[i] = <HTMLElement>this.htmlElements[i].parentElement.removeChild(this.htmlElements[i]);
        }
    }

    public destroy(): void {
        this.hide();
        this.htmlElements = null;
        this.parentElement = null;

        // TODO: Clear up bindings and such
    }
}