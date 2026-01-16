/*-----------------------------------------------------------------------------
| Copyright (c) 2014-2015, PhosphorJS Contributors
|
| Distributed under the terms of the BSD 3-Clause License.
|
| The full license is in the file LICENSE, distributed with this software.
|----------------------------------------------------------------------------*/
'use strict';


// temporary - replace with clear-cut
export
function isValidSelector(selector: string): boolean {
  return true;
}


// temporary - replace with clear-cut
export
function calculateSpecificity(selector: string): number {
  return 0;
}


/**
 * Test whether an element matches a CSS selector.
 *
 * @param elem - The element of interest.
 *
 * @param selector - A valid CSS selector.
 *
 * @returns `true` if the given element matches the CSS selector,
 *   `false` otherwise.
 */
export
function matchesSelector(elem: Element, selector: string): boolean {
  return protoMatchFunc.call(elem, selector);
}


/**
 * A cross-browser CSS selector matching prototype function.
 *
 * The function must be called with the element as `this`.
 */
var protoMatchFunc: Function = (() => {
  var proto = Element.prototype as any;
  return (
    proto.matches ||
    proto.matchesSelector ||
    proto.mozMatchesSelector ||
    proto.msMatchesSelector ||
    proto.oMatchesSelector ||
    proto.webkitMatchesSelector ||
    (function(selector: string) {
      var elem = this as Element;
      var matches = elem.ownerDocument.querySelectorAll(selector);
      return Array.prototype.indexOf.call(matches, elem) !== -1;
    })
  );
})();