/**
 * This file contains common functions that I could potentially apply
 * to future projects of any type.
 */

/**
 * From http://stackoverflow.com/a/901144/1405720
 * Retrieves the value of a named parameter from the URL.
 */
export function getParameterByName(name: string) {
    if (!_urlParams) _parseQueryString();
    return _urlParams[name];
}
var _urlParams: {};
function _parseQueryString() {
    var match: RegExpExecArray,
        pl     = /\+/g,  // Regex for replacing addition symbol with a space
        search = /([^&=]+)=?([^&]*)/g,
        decode = (s: string) => decodeURIComponent(s.replace(pl, " ")),
        query  = window.location.search.substring(1);

    _urlParams = {};
    while (match = search.exec(query))
        _urlParams[decode(match[1])] = decode(match[2]);
}