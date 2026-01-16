/**
 * KeyStroke structure
 */
export type KeyStroke = {
  /** with alt key */
  alt: boolean;
  /** code string */
  code: string;
  /** with ctrl key */
  ctrl: boolean;
  /** keystroke string if keystroke string is invalid, use for notice to user */
  invalid: string;
  /** with meta key */
  meta: boolean;
  /** with shift key */
  shift: boolean;
};

/**
 * Parse a keystroke string into an array of KeyStroke objects
 *
 * ```ts
 * parse("i"); // => [{ alt: false, code: "I", ctrl: false, invalid: "", meta: false, shift: false }]
 * parse("<C-x>"); // => [{ alt: false, code: "X", ctrl: true, invalid: "", meta: false, shift: false }]
 * ```
 */
export function parse(keystroke: string): KeyStroke[] {
  const keys = keystroke.match(/([a-z]|<[^>]*>)/gi);

  if (!keys) {
    return [];
  }

  const result = keys.map<KeyStroke>((key) => {
    let code = "";

    // is key within <>?
    const withinDiamond = key.match(/^<.*?-?([a-z]+)>$/i);

    if (withinDiamond && withinDiamond[1] !== "") {
      // <C-x> => x
      code = withinDiamond[1];
    } else if (key.length === 1) {
      // x => x
      code = key;
    }

    const invalid = code === "" ? key : "";

    return {
      alt: invalid ? false : /a-/i.test(key),
      code: code.toUpperCase(),
      ctrl: invalid ? false : /c-/i.test(key),
      invalid,
      meta: invalid ? false : /m-/i.test(key),
      shift: invalid ? false : /s-/i.test(key),
    };
  });

  return result;
}