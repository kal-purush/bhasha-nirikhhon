class HtmlView {
    private definedObjects: any = Object.create(null);

    public define(key: string, obj: any): void {
        this.definedObjects[key] = obj;
    }

    public parseHtml(): void {
        this.traverse(document.body);
    }

    private traverse(root: HTMLElement): void {
        const children = root.children;

        for (let i = 0; i < children.length; i++) {
            const element = <HTMLElement> children[i];

            if (element.hasAttribute("mk-value")) {
                const path = element.getAttribute("mk-value");
                const component = this.resolveComponent(path);
                component.attachElement(element);
            }

            // Continue recursive traversal through HTML element tree
            this.traverse(element);
        }
    }

    private resolveComponent(path: string): Component {
        const split = path.split(".");

        if (split.length !== 2) {
            // This limits the 'resolve' function greatly, but more
            // isn't really needed. (At least not currently)
            throw new Error("Expected path to have exactly two values split by a dot.");
        }

        const ref = this.definedObjects[split[0]];
        if (!ref) {
            throw new Error("Unresolved path.");
        }

        // Resolve path
        const cmp = <Component>  ref[split[1]];
        // And ensure value maps to a component
        if (!(cmp instanceof Component)) {
            throw new Error("Path does not map to a valid Component.");
        }

        return cmp;
    }
}