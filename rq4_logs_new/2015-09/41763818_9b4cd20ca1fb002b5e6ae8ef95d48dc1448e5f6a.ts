import Parser = require('./Parser');

function findFieldInNode(node: Parser.Node, field: string): Parser.Node[] {
    var result = [];
    if (node.type === Parser.NodeType.CLASS_FIELD || node.type === Parser.NodeType.ROOT) {
        result = findFieldInArray(node.fields, field);
    }
    return result;
}

function findFieldInArray(nodes: Parser.Node[], field: string): Parser.Node[] {
    var result: Parser.Node[] = [];
    for (var i = 0, len = nodes.length; i < len; i++) {
        if (nodes[i].fieldName === field || matchAllFields(nodes[i].fieldName, field)) {
            result.push(nodes[i]);
        }
    }
    return result;
}

function matchAllFields(field: string, pattern: string): boolean {
    return pattern.indexOf('*') === pattern.length - 1 &&
        field.indexOf(pattern.substring(0, pattern.length - 1)) === 0;
}

export function addEmptyClassNode(node: Parser.Node, className: string): Parser.Node {
    var newClass: Parser.Node = {
        fieldName: className,
        inheritsFrom: '',
        fields: [],
        type: Parser.NodeType.CLASS_FIELD
    };
    node.fields.push(newClass);
    return newClass;
}

export function addLiteralNode(node: Parser.Node, fieldName, value, type): Parser.Node {
    var newNode: Parser.Node = {
        fieldName: fieldName,
        type: type,
        value: value
    };
    node.fields.push(newNode);
    return newNode;
}

export function select(ast: Parser.Node, selector: string): Parser.Node[] {
    var fields: string[] = selector.split('.'),
        result: Parser.Node[] = [],
        node: Parser.Node = ast;
    for (var i = 0, len = fields.length; i < len; i++) {
        result = findFieldInNode(node, fields[i]);
        if (result.length == 1) {
            node = result[0];
        } else if (i !== fields.length - 1) {
            return [];
        }
    }
    return result;
}

export function createClass(ast: Parser.Node, selector: string, className: string): Parser.Node {
    var nodes: Parser.Node[] = select(ast, selector + '.' + className),
        result: Parser.Node = null;
    if (nodes.length > 0) {
        result = nodes[0];
    } else {
        result = addEmptyClassNode(select(ast, selector)[0], className);
    }
    return result;
}