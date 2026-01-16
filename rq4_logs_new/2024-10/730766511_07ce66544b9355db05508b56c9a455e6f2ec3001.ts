import Yaml from "js-yaml";
import en_us from "../i18n/en-us.yaml";
import eu from "../i18n/eu.yaml";
import fr from "../i18n/fr.yaml";
import hu from "../i18n/hu.yaml";
import it from "../i18n/it.yaml";
import ja from "../i18n/ja.yaml";
import nl from "../i18n/nl.yaml";
import pt_br from "../i18n/pt-br.yaml";
import pt from "../i18n/pt.yaml";
import { TranslationNotFoundError } from "./errors";
import { expFormat, getTranslation } from "./utils";

test("expFormat convert written text to format well in HTML", () => {
  expect(expFormat("abcdefg")).toStrictEqual("abcdefg");
  expect(expFormat("AAABBBCCC")).toStrictEqual("AAABBBCCC");
  expect(expFormat("A normal sentence with multiple words.")).toStrictEqual(
    "A normal sentence with multiple words.",
  );
  expect(expFormat(["Array", "of", "strings"])).toStrictEqual(
    "Array<br><br>of<br><br>strings",
  );
  expect(expFormat("carriage return an newline\r\n")).toStrictEqual(
    "carriage return an newline<br>",
  );
  expect(expFormat("new line\n")).toStrictEqual("new line<br>");
  expect(expFormat("carriage return\r")).toStrictEqual("carriage return<br>");
  expect(expFormat("\tTabbed text")).toStrictEqual(
    "&nbsp;&nbsp;&nbsp;&nbsp;Tabbed text",
  );
});

test("Get translation file for specified locale", () => {
  const translations = {
    ja,
    pt,
    eu,
    fr,
    hu,
    it,
    nl,
    "en-us": en_us,
    "pt-br": pt_br,
  };

  for (const [k, v] of Object.entries<string>(translations)) {
    expect(getTranslation(new Intl.Locale(k))).toStrictEqual(Yaml.load(v));
  }

  expect(pt_br).not.toStrictEqual(pt);

  expect(() => getTranslation(new Intl.Locale("not-a2code"))).toThrow(
    TranslationNotFoundError,
  );
});