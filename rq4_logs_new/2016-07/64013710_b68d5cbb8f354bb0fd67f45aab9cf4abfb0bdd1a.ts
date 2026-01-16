import removePreviousBuffer from '../lib/remove-previous-buffer';
import stringTrim from '../lib/string-trim';

function currency(input: string): string {
    return input.replace(/(?:([0-9]+?[\.\,]+?[0-9]+?)|([0-9]+?))(?:\s|[^0-9]|$)/g, (match: string, floatString: string): string => {
        if (navigator.language.toLowerCase().indexOf('de') > -1) {
            if (floatString.indexOf(',') > -1) {
                return parseFloat(floatString.replace(',', '.')).toFixed(2).replace('.', ',');
            } else {
                return parseFloat(floatString).toFixed(2).replace('.', ',');
            }
        } else {
            if (floatString.indexOf(',') > -1) {
                return parseFloat(floatString.replace(',', '.')).toFixed(2);
            } else {
                return parseFloat(floatString).toFixed(2);
            }
        }
    });
}

export default function (name: string, input: string, buffer: Templata.Object.Buffer, compiler: Templata.Interface.Compiler): string {
    compiler.registerImport('__f_currency', currency);

    return buffer.APPEND + '__f_currency(' + stringTrim(removePreviousBuffer(input, buffer)) + ')' + buffer.POST_APPEND;
}