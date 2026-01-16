// export const getLanguages = async (token) => {
//   const response = await fetch(`${APIPath}`, {
//     method: "post",
//     headers: {
//       "Content-Type": "application/json",
//       headers: { Authorization: token },
//     },
//   });
//   const result = await response.json();
//   console.log("__", result);
// };

export interface LangI{
  id: string;
  name: string;
  localName: string;
  locale: string;
  localeFull: string;
  isRtl: boolean;
}

export const getLanguagesList = () => {
  return [{ "id": 13, "name": "Arabic", "localName": "العربية", "locale": "AR", "localeFull": "ar-AE", "isRtl": true }, { "id": 3, "name": "German", "localName": "Deutsch", "locale": "DE", "localeFull": "de-DE", "localeMoment": "de", "isRtl": false }, { "id": 2, "name": "English", "localName": "English", "locale": "en", "localeFull": "en-US", "isRtl": false }, { "id": 41, "name": "Spanish (Latin America)", "localName": "Español", "locale": "es", "localeFull": "es-LA", "localeMoment": "es", "isRtl": false }, { "id": 7, "name": "French", "localName": "Français", "locale": "FR", "localeFull": "fr-FR", "localeMoment": "fr", "isRtl": false }, { "id": 17, "name": "Italian", "localName": "Italiano", "locale": "IT", "localeFull": "it-IT", "localeMoment": "it", "isRtl": false }, { "id": 44, "name": "Japanese", "localName": "日本語", "locale": "JA", "localeFull": "ja-JP", "localeMoment": "ja", "isRtl": false }, { "id": 43, "name": "Korean", "localName": "한국어", "locale": "KO", "localeFull": "ko-KR", "localeMoment": "ko", "isRtl": false }, { "id": 21, "name": "Portuguese (Brasil)", "localName": "Português", "locale": "PT", "localeFull": "pt-BR", "localeMoment": "pt-br", "isRtl": false }, { "id": 9, "name": "Russian", "localName": "Русский", "locale": "RU", "localeFull": "ru-RU", "localeMoment": "ru", "isRtl": false }, { "id": 25, "name": "Swedish", "localName": "Svenska", "locale": "SV", "localeFull": "sv-SE", "localeMoment": "sv", "isRtl": false }, { "id": 16, "name": "Turkish", "localName": "Türkçe", "locale": "TR", "localeFull": "tr-TR", "localeMoment": "tr", "isRtl": false }, { "id": 35, "name": "Chinese (Simplified)", "localName": "简体中文", "locale": "zh", "localeFull": "zh-Hans", "localeMoment": "zh-cn", "isRtl": false }, { "id": 34, "name": "Chinese (Traditional)", "localName": "繁體中文", "locale": "zh", "localeFull": "zh-TW", "localeMoment": "zh-tw", "isRtl": false }]
}