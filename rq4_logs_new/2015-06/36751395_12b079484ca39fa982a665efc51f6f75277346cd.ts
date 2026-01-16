/// <reference path="type_declarations/DefinitelyTyped/virtual-dom/virtual-dom.d.ts" />
import {VNode} from 'virtual-dom';
declare module xmltree {
    class XMLRenderer {
        protected exclude: string[];
        constructor(exclude?: string[]);
        /**
        Check the given name against the XMLRenderer's `exclude` blacklist.
        Return false for element nodes which have a blacklisted tag name.
        Return true if the name is allowed (does not occur in the blacklist).
        */
        protected isIncluded(name: string): boolean;
        protected renderAttributes(attributes: NamedNodeMap): VNode[];
        protected renderAttribute(attribute: Attr): VNode;
        protected renderNodes(nodes: NodeList): VNode[];
        protected renderNode(node: Node): VNode;
        render(xml: string): VNode;
        /**
        Creates an HTML element and appends it to container if the given `vtree` is
        undefined; otherwise, updates element in place. `element` should not be
        manipulated without also reflecting the changes in `vtree`.
        */
        update(xml: string, container: Node, element: Element, vtree: VNode): (Element | VNode)[];
    }
}