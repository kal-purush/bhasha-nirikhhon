`use strict`
 // http://www.lingoes.net/en/translator/langcode.htm
export enum KoconutLocale {

    // English
    'en' = 'en',        // English
    'en-AU' = 'en-AU',  // Australia
    'en-BZ' = 'en-BZ',  // Belize
    'en-CA' = 'en-CA',  // Canada
    'en-CB' = 'en-CB',  // Caribbean
    'en-GB' = 'en-GB',  // United Kingdom
    'en-IE' = 'en-IE',  // Ireland
    'en-JM' = 'en-JM',  // Jamaica
    'en-NZ' = 'en-NZ',  // New Zealand
    'en-PH' = 'en-PH',  // Republic of the Philippines
    'en-TT' = 'en-TT',  // Trinidad and Tobago
    'en-US' = 'en-US',  // United States
    'en-ZA' = 'en-ZA',  // South Africa
    'en-ZW' = 'en-ZW',  // Zimbabwe

    // Japanese
    'ja' = 'ja',        // Japanese,
    'ja-JP' = 'ja-JP',  // Japan

    // Korean
    'ko' = 'ko',        // Korean
    'ko-KR' = 'ko-KR',  // Korea

}
const localeNames = Object.values(KoconutLocale)
const upperCaseLocaleNames = localeNames
                            .map(eachName => eachName.toString().toUpperCase())
export namespace KoconutLocale {
    export function fromString(localeString : string) : KoconutLocale {
        return upperCaseLocaleNames.includes(localeString.toUpperCase())
            ? localeNames[upperCaseLocaleNames.indexOf(localeString.toUpperCase())] as KoconutLocale
            : KoconutLocale["en-US"]
    }
}