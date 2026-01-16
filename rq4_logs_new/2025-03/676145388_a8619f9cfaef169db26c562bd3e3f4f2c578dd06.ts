import "server-only";
import { Locale } from "@/utils/i18n/languages";

export type { Dictionary };

const dictionaries: Record<Locale, () => Promise<Dictionary>> = {
  en: () => import("./dictionaries/en.json").then((module) => module.default),
  es: () => import("./dictionaries/es.json").then((module) => module.default),
  fr: () => import("./dictionaries/fr.json").then((module) => module.default),
  de: () => import("./dictionaries/de.json").then((module) => module.default),
  ca: () => import("./dictionaries/ca.json").then((module) => module.default),
  hr: () => import("./dictionaries/hr.json").then((module) => module.default),
  cs: () => import("./dictionaries/cs.json").then((module) => module.default),
  nl: () => import("./dictionaries/nl.json").then((module) => module.default),
  lt: () => import("./dictionaries/lt.json").then((module) => module.default),
  pl: () => import("./dictionaries/pl.json").then((module) => module.default),
  pt: () => import("./dictionaries/pt.json").then((module) => module.default),
  ro: () => import("./dictionaries/ro.json").then((module) => module.default),
  el: () => import("./dictionaries/el.json").then((module) => module.default),
};

export async function getDictionary(locale: Locale): Promise<Dictionary> {
  return dictionaries[locale]();
}

type Dictionary = {
  navigation: {
    scrollDocumentary: string;
    narratives: string;
    nonLinearNarratives: string;
    freeBrowsing: string;
    project: string;
    team: string;
    events: string;
    educationalResources: string;
    researchPublication: string;
    travellingWorkshop: string;
    contact: string;
  };
  landing: {
    description: string;
    scrollDocumentary: string;
    narratives: string;
    freeBrowsing: string;
  };
  about: {
    p1: string;
    p2: string;
  };
  team: {
    description: string;
    teamList: Array<{
      organisation: string;
      members: string[];
      country: string;
    }>;
  };
  contact: {
    getInTouch: string;
    download: string;
  };
  narratives: {
    title: string;
    select: string;
    watch: string;
    continue: string;
    switch: string;
  };
  freeBrowsing: {
    title: string;
    description: string;
    explanation: string;
  };
  eduPack: {
    title: string;
    subtitle: string;
    lead: string;
    p1: string;
    p2: string;
    questions: string[];
    p3: string;
    download: string;
  };
  researchPublication: {
    title: string;
    subtitle: string;
    lead: string;
    p1: string;
    download: string;
  };
  travellingWorkshop: {
    title: string;
    lead: string;
    p1: string;
    p2: string;
    p3: string;
    p4: string;
    p5: string;
  };
};