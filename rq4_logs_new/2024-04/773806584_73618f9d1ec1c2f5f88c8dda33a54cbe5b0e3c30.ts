/**
 * Used on database strings
 * Unsanitizes user submitted inputs from database
 * Unsanitize: e.g '&lt;' becomes '<' or '&gt;' becomes '>'
 */

function parseDBString(string:string): string {
    return string.replace(/\&lt;/g, "<").replace(/\&gt;/g, ">").replace(/\&#x2F;/g, "/").replace(/\&quot;/g, `"`);
}

export {parseDBString}