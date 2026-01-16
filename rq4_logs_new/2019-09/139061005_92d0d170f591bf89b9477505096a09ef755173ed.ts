import queryString from "query-string"
import { Locale } from "stores/TranslationStore"

export function parseLocaleParam(search: string) {
    const lang = queryString.parse(search).lang
    if (lang === 'fi') {
        return Locale.FI
    } else if (lang === 'sv') {
        return Locale.SV
    } else {
        return ''
    }
}