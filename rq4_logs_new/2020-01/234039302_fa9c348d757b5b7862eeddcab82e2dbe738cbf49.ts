/**
 * Removes an element from the DOM
 *
 * @export
 * @param {Element} element
 */
export declare function removeElement(element: Element): void;
/**
 * Inserts the element after the reference element.
 *
 * @export
 * @param {Element} elementToInsert
 * @param {Element} referenceElement
 */
export declare function insertAfter(elementToInsert: Element, referenceElement: Element): void;
/**
 * Inserts the element before the reference element
 *
 * @export
 * @param {Element} elementToInsert
 * @param {Element} referenceElement
 */
export declare function insertBefore(elementToInsert: Element, referenceElement: Element): void;
/**
 * Add's an element to the start of the parentElement
 *
 * @export
 * @param {Element} elementToInsert
 * @param {Element} parentElement
 */
export declare function prependElement(elementToInsert: Element, parentElement: Element): void;
/**
 * Add's an element to the end of the parentElement
 *
 * @export
 * @param {Element} elementToInsert
 * @param {Element} parentElement
 */
export declare function appendElement(elementToInsert: Element, parentElement: Element): void;