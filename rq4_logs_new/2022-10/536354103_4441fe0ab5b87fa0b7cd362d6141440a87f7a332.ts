import { atom, useRecoilState } from "recoil";

const urlEncodedTextState = atom<string>({
  key: "urlEncodedTextState",
  default: "",
});

const urlDecodedTextState = atom<string>({
  key: "urlDecodedTextState",
  default: "",
});

const base64EncodedTextState = atom<string>({
  key: "base64EncodedTextState",
  default: "",
});

const base64DecodedTextState = atom<string>({
  key: "base64DecodedTextState",
  default: "",
});

const convertedJsonFromYamlState = atom<string>({
  key: "convertedJsonFromYamlState",
  default: "",
});

const convertedYamlFromJsonState = atom<string>({
  key: "convertedYamlFromJsonState",
  default: "",
});

const formattedHtmlState = atom<string>({
  key: "formattedHtmlState",
  default: "",
});

const unformattedHtmlState = atom<string>({
  key: "unformattedHtmlState",
  default: "",
});

const formattedCssState = atom<string>({
  key: "formattedCssState",
  default: "",
});

const unformattedCssState = atom<string>({
  key: "unformattedCssState",
  default: "",
});

const formattedJavaScriptState = atom<string>({
  key: "formattedJavaScriptState",
  default: "",
});

const unformattedJavaScriptState = atom<string>({
  key: "unformattedJavaScriptState",
  default: "",
});

const formattedSqlState = atom<string>({
  key: "formattedSqlState",
  default: "",
});

const unformattedSqlState = atom<string>({
  key: "unformattedSqlState",
  default: "",
});

const formattedXmlState = atom<string>({
  key: "formattedXmlState",
  default: "",
});

const unformattedXmlState = atom<string>({
  key: "unformattedXmlState",
  default: "",
});

const formattedJsonState = atom<string>({
  key: "formattedJsonState",
  default: "",
});

const unformattedJsonState = atom<string>({
  key: "unformattedJsonState",
  default: "",
});

const formattedYamlState = atom<string>({
  key: "formattedYamlState",
  default: "",
});

const unformattedYamlState = atom<string>({
  key: "unformattedYamlState",
  default: "",
});

const hashTextState = atom<string>({
  key: "hashTextState",
  default: "",
});

const qrCodeTextState = atom<string>({
  key: "qrCodeTextState",
  default: "",
});

export const atoms = {
  urlEncodedText: urlEncodedTextState,
  urlDecodedText: urlDecodedTextState,
  base64EncodedText: base64EncodedTextState,
  base64DecodedText: base64DecodedTextState,
  convertedJsonFromYaml: convertedJsonFromYamlState,
  convertedYamlFromJson: convertedYamlFromJsonState,
  formattedHtml: formattedHtmlState,
  unformattedHtml: unformattedHtmlState,
  formattedCss: formattedCssState,
  unformattedCss: unformattedCssState,
  formattedJavaScript: formattedJavaScriptState,
  unformattedJavaScript: unformattedJavaScriptState,
  formattedSql: formattedSqlState,
  unformattedSql: unformattedSqlState,
  formattedXml: formattedXmlState,
  unformattedXml: unformattedXmlState,
  formattedJson: formattedJsonState,
  unformattedJson: unformattedJsonState,
  formattedYaml: formattedYamlState,
  unformattedYaml: unformattedYamlState,
  hashText: hashTextState,
  qrCodeText: qrCodeTextState,
} as const;

export type AtomKey = keyof typeof atoms;

export const useStore = (key: AtomKey) => {
  return useRecoilState(atoms[key]);
};