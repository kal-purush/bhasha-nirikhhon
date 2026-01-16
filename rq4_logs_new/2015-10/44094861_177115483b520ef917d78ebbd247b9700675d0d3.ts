import {Provider} from './Provider';

let provider = Provider.instance;

export class Compiler {

    static _instance: Compiler;

    static get instance(): Compiler {
        if (!Compiler._instance) {
            Compiler._instance = new Compiler();
        }

        return Compiler._instance;
    }

    bootstrap() {
        this.compile(document.body, provider.get('$rootScope'));
    }

    compile(element: string, scope: any);
    compile(element: Element, scope: any);
    compile(element: any, scope: any) {
        if (typeof element === 'string') {
            let parser = new DOMParser();
            element = parser.parseFromString(element, 'text/xml').documentElement;
        }

        let directives = this._getElementDirectives(element);
        let scopeCreated;

        directives.forEach((d) => {
            let directive = provider.get(d.name + Provider.DIRECTIVE_SUFFIX);
            if (directive.scope && !scopeCreated) {
                scope = scope.$new();
                scopeCreated = true;
            }
            directive.link(element, scope, d.value);
        });

        let children = Array.prototype.slice.call(element.children);

        children.forEach((child) => this.compile(child, scope));

        return element;
    }

    private _getElementDirectives(element: Element): Array<any> {
        let attrs = element.attributes;
        let result = [];

        for (var i = 0; i < attrs.length; i += 1) {
            if (provider.get(attrs[i].name + Provider.DIRECTIVE_SUFFIX)) {
                result.push({
                    name: attrs[i].name,
                    value: attrs[i].value
                });
            }
        }
        return result;
    }

};