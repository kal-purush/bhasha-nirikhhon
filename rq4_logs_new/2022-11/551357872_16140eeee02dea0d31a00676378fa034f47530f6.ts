import {encodeXmlSpecialChars, mapMaterialType} from "./utils";


test("escape ampersand", () => {
    const weirdtitle = "Hest & pony : pleje & pasning";
    const actual = encodeXmlSpecialChars(weirdtitle);
    const expected = "Hest &amp; pony : pleje &amp; pasning"
    expect(actual).toEqual(expected);
})

test("escape all specialchars",() => {
    const weirdtitle = "Hest < ' & pony >  : pleje & pasning";
    const actual = encodeXmlSpecialChars(weirdtitle);
    const expected = "Hest &lt; ' &amp; pony &gt;  : pleje &amp; pasning"
    expect(actual).toEqual(expected);
})

test("map MaterialTypes",() => {
    const matType = "lydbog (net)";
    const actual = mapMaterialType(matType);
    const expected = "Lydbog"
    expect(actual).toEqual(expected);
})