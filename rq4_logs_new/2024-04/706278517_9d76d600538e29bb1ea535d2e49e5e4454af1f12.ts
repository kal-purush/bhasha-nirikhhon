import { Alpha3Code, alpha2ToAlpha3, getNames } from 'i18n-iso-countries'

const CountryNames = getNames('en')

export const AllCountries: Array<{ code: Alpha3Code; name: string }> = Object.keys(CountryNames)
    .map((key) => {
        const alpha3code = alpha2ToAlpha3(key)
        if (!alpha3code) return null

        return {
            code: alpha3code as Alpha3Code,
            name: Array.isArray(CountryNames[key]) ? CountryNames[key][0] : CountryNames[key]
        }
    })
    .filter((c: { code: Alpha3Code; name: string } | null) => c !== null) as Array<{
    code: Alpha3Code
    name: string
}>