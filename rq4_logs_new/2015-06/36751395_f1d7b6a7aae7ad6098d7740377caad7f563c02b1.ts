/// <reference path="type_declarations/DefinitelyTyped/virtual-dom/virtual-dom.d.ts" />
import {VNode, VChild, h, create, diff, patch} from 'virtual-dom';

export class XMLRenderer {
  constructor(protected exclude: string[] = []) { }

  /**
  Check the given name against the XMLRenderer's `exclude` blacklist.
  Return false for element nodes which have a blacklisted tag name.
  Return true if the name is allowed (does not occur in the blacklist).
  */
  protected isIncluded(name: string): boolean {
    return this.exclude.indexOf(name) === -1;
  }

  protected renderAttributes(attributes: NamedNodeMap): VNode[] {
    var vnodes: VNode[] = [];
    var attribute: Attr;
    for (attribute of attributes) {
      if (this.isIncluded(attribute.name)) {
        vnodes.push(this.renderAttribute(attribute));
      }
    }
    return vnodes;
  }

  protected renderAttribute(attribute: Attr): VNode {
    return h('span.attribute', [
      ' ',
      h('span.name', attribute.name),
      '=',
      h('span.value', `"${attribute.value}"`),
    ]);
  }

  protected renderNodes(nodes: NodeList): VNode[] {
    var vnodes: VNode[] = [];
    var node: Node;
    for (node of nodes) {
      if (node.nodeType === Node.ELEMENT_NODE && this.isIncluded((<Element>node).tagName)) {
        vnodes.push(this.renderNode(node));
      }
    }
    return vnodes;
  }

  protected renderNode(node: Node): VNode {
    if (node.nodeType == Node.TEXT_NODE) {
      var text = <Text>node;
      return h('div.text', text.data);
    }
    else if (node.nodeType == Node.ELEMENT_NODE) {
      var element = <Element>node;
      var tagName = element.tagName;
      var startTagChildren: VChild[][] = [['<', tagName], this.renderAttributes(element.attributes), ['>']];
      var startTag = h('span.start', Array.prototype.concat.apply([], startTagChildren));
      var endTag = h('span.end', {}, ['</', tagName, '>']);
      return h('div.element', [startTag, this.renderNodes(node.childNodes), endTag]);
    }
    else {
      return h('span', `(Ignoring node type = ${node.nodeType})`);
    }
  }

  render(xml: string): VNode {
    var document = new DOMParser().parseFromString(xml, 'application/xml');
    return h('div.xmltree', this.renderNodes(document.childNodes));
  }

  /**
  Creates an HTML element and appends it to container if the given `vtree` is
  undefined; otherwise, updates element in place. `element` should not be
  manipulated without also reflecting the changes in `vtree`.
  */
  update(xml: string, container: Node, element: Element, vtree: VNode) {
    var new_vtree = this.render(xml);
    if (vtree === undefined) {
      vtree = new_vtree;
      element = create(vtree);
      container.appendChild(element);
    }
    else {
      var patches = diff(vtree, new_vtree)
      element = patch(element, patches)
      vtree = new_vtree;
    }
    return [element, vtree];
  }
}