/**
 * Format a number into currency format. Example, 1.23 => $1.23, or 1 => $1.00.
 * @param number value
 */
export function formatPrice(val: any) {
  let t = Number(val);
  if (!isNaN(t))
    return '$' + t.toFixed(2);
  else
    return '';
}

/**
 * Create a Foundation Grid Column Div HTML Node. Note: 'columns' class is already added for you.
 * @param classes
 */
export function createColumnDivNode(classes: string[]): HTMLDivElement {
  let col = document.createElement('div');

  classes.push('columns');
  classes.forEach(c => { col.classList.add(c); });
    
  return col;
}

/**
 * Updates scene7 image URL with specific height, width, and/or makes it a progressive JPEG.
 * @param Scene7 url string
 * @param width number
 * @param height number
 * @param convertToJpeg Update string to be a JPEG.
 */
export function updateScene7ImageUrl(url: string, width = 0, height = 0, convertToJpeg = true): string {
  if (typeof url !== 'string') return url;
  
  url = url.replace('http:', 'https:');

  if (width > 0)
    url = url.replace(/wid=[0-9]{1,5}/, 'wid=' + width.toString());

  if (height > 0)
    url = url.replace(/hei=[0-9]{1,5}/, 'hei=' + height.toString());

  if (convertToJpeg)
    url = url.replace(/fmt=jpg/, 'fmt=pjpg');
  
  return url;
}

export interface QueryString {
  name: string;
  value: string;
}

export interface IQueryStringReader {
  getAttributesAndValues: Function;
}

/**
 * Class which handles extraction of name value pairs from a query string.
 */
export class QueryStringReader implements IQueryStringReader {
  private queryString: string;

  /**
   * Takes in the window query string and attempts to find the given name.
   * If not found, will return NULL.
   * @param name The name of the query string to find.
   */
  public static findQueryString(name: string): QueryString {
    let qs: QueryString = null;
    const qsr = new QueryStringReader();

    qsr.getAttributesAndValues().forEach(q => {
      if (q.name === name)
        qs = q;
    });

    return qs;
  }
  
  /**
   * Create a query string reader.
   * @param queryString? string
   */
  constructor(queryString?: string) {
    if (null === queryString || undefined === queryString)
      this.queryString = window.location.search;
    else
      this.queryString = queryString;
    
    this.queryString = decodeURI(this.queryString);
  }
  
  /**
   * Extract the name value pairs from a query string.
   * @return QueryString[]
   */
  public getAttributesAndValues(): QueryString[] {
    const nameValuePairs = this.queryString.substr(1).split('&'),
          queryStringValues: QueryString[] = [];
    
    nameValuePairs.forEach(pair => {
      const items = pair.split('=');
      
      queryStringValues.push({
        name: items[0],
        value: items[1]
      });
    });
    
    return queryStringValues;
  }
}

/**
 * Determines whether or not the current screen is a small screen.
 */
export function isSmallScreen(): boolean {
  const width = document.documentElement.clientWidth;
  return width <= 640;
}

/**
 * Determines whether or not the current screen is a medium screen.
 */
export function isMediumScreen(): boolean {
  const width = document.documentElement.clientWidth;
  return !isSmallScreen() && width <= 1024;
}

/**
 * Determines whether or not the current screen is a large screen.
 */
export function isLargeScreen(): boolean {
  const width = document.documentElement.clientWidth;
  return width > 1024;
}

/**
 * Determines whether or not a given element is currently completely visible inside the viewport
 */
export function isFullyInViewport(element: Element): boolean {
  const coordinates = element.getBoundingClientRect();
  if (null === element || undefined === element) return false;
  
  return (
    coordinates.top >= 0 &&
    coordinates.left >= 0 &&
    coordinates.bottom <= window.innerHeight &&
    coordinates.right <= window.innerWidth
    );
}

/**
 * Determines whether or not the top of a given element is currently visible inside the viewport
 */
export function isTopInViewport(element: Element): boolean {
  const coordinates = element.getBoundingClientRect();
  if (null === element || undefined === element) return false;
  
  return (
    coordinates.top >= 0 &&
    coordinates.left >= 0 &&
    coordinates.top <= window.innerHeight &&
    coordinates.right <= window.innerWidth
  );
}

/**
 * Determines whether or not the bottom of a given element is currently visible inside the viewport
 */
export function isBottomInViewport(element: Element): boolean {
  const coordinates = element.getBoundingClientRect();
  
  return (
    coordinates.bottom <= window.innerHeight &&
    coordinates.right <= window.innerWidth
  );
}

/**
 * Shows a given element.
 * @param cssDisplay = 'block'
 */
export function show(element: HTMLElement, cssDisplay = 'block'): void {
  element.style.display = cssDisplay;
}

/**
 * Hides a given element.
 */
export function hide(element: HTMLElement): void {
  element.style.display = 'none';
}

/**
 * Toggles the display status of an element between visible and hidden.
 * @param cssDisplay = 'block'
 */
export function toggleDisplay(element: HTMLElement, cssDisplay = 'block'): void {
  if (element.style.display === 'none')
    element.style.display = cssDisplay;
  else
    element.style.display = 'none';
}

/**
 * Toggles the innerHTML of an element between two strings.
 */  
export function toggleText(element: HTMLElement, textOptions: string[]): void {
  if (element.innerHTML === textOptions[0])
    element.innerHTML = textOptions[1];
  else
    element.innerHTML = textOptions[0];
}

/**
 * Finds the closest parent node. Similar to jQuery's .closest function.
 * @param el Initial element
 * @param selector Selector to find
 * @return Node of found parent. If none found, return input node.
 */
export function closest(el, selector: string): HTMLElement {
  const matchesSelector = el.matches || el.webkitMatchesSelector || el.mozMatchesSelector || el.msMatchesSelector;
  
  while (el) {
    if (matchesSelector.call(el, selector)) break;
    el = el.parentElement;
  }
  return el;
}

/**
 * Pad a string with zeroes.
 * @param n string to pad
 * @param width how many digits
 * @param z string to prepend
 */
export function pad(n: string, width: number, z?: string): string {
  z = z || '0';
  n = n + '';
  return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

/**
 * Figure out if user is trying to open a new tab with a click.
 */
export function isNewTabClick(e: MouseEvent) {
  return (
        e.ctrlKey || 
        e.shiftKey || 
        e.metaKey || // apple
        (e.button && e.button === 1) // middle click, >IE9 + everyone else
    );
}

/**
 * Contains utility functions for verifying the validity of page elements.
 */
export class ElementValidator {
  /**
   * Checks a single element against null and undefined.
   */
  public static isValidElement(element: Element): boolean {
    return null !== element && undefined !== element;
  }
  
  /**
   * Checks an array of elements against null and undefined.
   * @param elements
   */
  public static areValidElements(elements: Element[]): boolean {
    for (const el of elements)
      if (null === el || undefined === el)
        return false;
    return true;
  }
}