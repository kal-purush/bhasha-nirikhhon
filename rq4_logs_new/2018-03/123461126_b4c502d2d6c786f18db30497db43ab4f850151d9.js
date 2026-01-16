import en from "./languages/english.js"
import fr from "./languages/french.js"
// import sp from "./languages/spanish.js"

export default class Translate{
  constructor(){
    this.localeData = {}
    this.browserLanguageCode = (
      typeof navigator === "undefined"
      ? "en"
      : (navigator.languages && navigator.languages[0]) || navigator.language || navigator.userLanguage
    ).toLowerCase().split(/[_-]+/)[0]
  }

  registerLanguage(languageFile){
    if (!languageFile){
      console.error('Language File not found')
    }
    this.localeData[languageFile.languageCode] = languageFile
  }

  getLocale(laguageCode){
    if (!this.localeData[laguageCode]){
      console.error('Language Definition Not Found')
    }
    return this.localeData[laguageCode]
  }

  message(key, languageCode = this.browserLanguageCode){
    if (!this.localeData[this.browserLanguageCode].messages[key]){
      console.error(`The message ${key} for language code ${this.browserLanguageCode} was not found`)
    }
    return this.localeData[this.browserLanguageCode].messages[key]
  }
}