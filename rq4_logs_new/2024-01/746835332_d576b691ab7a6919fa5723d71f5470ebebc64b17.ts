/**
 * Joins the provided class names into a single string, filtering out any falsy values.
 * 
 * @param {...(string | undefined | null | false | 0)} classes - The class names to join.
 * @returns {string} A string containing the joined class names.
 * @example
 * // Example usage:
 * const className = classNames(
 *   item.current
 *     ? 'bg-gray-900 text-white'
 *     : 'text-gray-300 hover:bg-gray-700 hover:text-white',
 *   'rounded-md px-3 py-2 text-sm font-medium'
 * );
 */
function classNames(...classes: (string | undefined | null | false | 0)[]): string {
    return classes.filter(Boolean).join(" ");
  }
export default classNames;