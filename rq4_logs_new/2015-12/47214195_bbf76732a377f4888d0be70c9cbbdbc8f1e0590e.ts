import defaultLanguage from "res/default";
import * as ko from "knockout";

export var text = defaultLanguage;
export var keys:{[key:string]:string} = {};

for (var key in text) {
    var mod = text[key];
    for (var sub in mod) {
        var line = mod[sub];
        keys[key + '.' + sub] = line;
    }
}

// A binding handler that set the text content with a format ({n} is replaced by the nth parameter)
ko.bindingHandlers['locf'] = {
    update: (element: HTMLElement, valueAccessor: () => any[]) => {
        var parameters = valueAccessor();
        var text = keys[parameters[0]];
        for (var i = 1; i < parameters.length; i++) {
            text = text.replace('{' + (i - 1) + '}', ko.unwrap(parameters[i]));
        }
        element.innerHTML = text;
    }
}

// A binding handler to set attribute values
ko.bindingHandlers['loca'] = {
    update: (element: HTMLElement, valueAccessor: () => any) => {
        var parameters = valueAccessor();
        for (var key in parameters) {
            var value = parameters[key];
            element.setAttribute(key, keys[value]);
        }
    }
};

ko.bindingProvider.instance['preprocessNode'] = function (node: Node) {
    if (node.nodeType === node.ELEMENT_NODE) {
        var element = <HTMLElement>node;
        for (var i = 0; i < element.attributes.length; i++) {
            var attribute = element.attributes[i];
            var value = attribute.value;
            if (value.indexOf("{{") == 0) {
                value = value.substr(2, value.length - 4);
                element.removeAttribute(attribute.name);
                if (element.hasAttribute('data-bind')) {
                    element.attributes.getNamedItem('data-bind').value += ',loca: {' + attribute.name + ':"' + value + '"}';
                }
                else {
                    element.setAttribute('data-bind', 'loca: {' + attribute.name + ':"' + value + '"}');
                }
            }
        }
    }

    if (node.nodeType === 3) {

        if (node.nodeValue && node.nodeValue.indexOf('{{nohtml|') !== -1) {
            node.nodeValue = node.nodeValue.replace(/{{nohtml\|(.*?)}}/, (match, id) => keys[id] || id);
            return [node];
        }
        else if (node.nodeValue && node.nodeValue.indexOf('{{') !== -1) {
            if (node.nodeValue.indexOf(',') !== -1) {
                var newElement = document.createElement('span');
                var value = node.nodeValue.match(/{{(.*?)}}/)[1];
                var parameters = value.split(',');
                parameters[0] = "'" + parameters[0] + "'";
                newElement.setAttribute('data-bind', 'locf: [' + parameters.join(',') + ']');
                node.parentNode.replaceChild(newElement, node);
                return [newElement];
            }
            else {
                var newElement = document.createElement('span');
                newElement.innerHTML = node.nodeValue.replace(/{{(.*?)}}/, (match, id) => keys[id] || id);
                node.parentNode.replaceChild(newElement, node);
                return [newElement];
            }
        }
    }
}