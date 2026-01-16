import { VEGA_EMBED_2, VEGA_EMBED_4 } from "./embedders";
import { makeTransform } from "./transform";
import { VegaVersion } from "./types";

export const VERSIONS: Record<string, VegaVersion<string>> = {
  VEGA2: {
    kind: "vega",
    version: "2",
    mediaType: "application/vnd.vega.v2+json",
    schemaPrefix: "https://vega.github.io/schema/vega/v2",
  },
  VEGA3: {
    kind: "vega",
    version: "3",
    mediaType: "application/vnd.vega.v3+json",
    schemaPrefix: "https://vega.github.io/schema/vega/v3",
  },
  VEGA4: {
    kind: "vega",
    version: "4",
    mediaType: "application/vnd.vega.v4+json",
    schemaPrefix: "https://vega.github.io/schema/vega/v4",
  },
  VEGA5: {
    kind: "vega",
    version: "5",
    mediaType: "application/vnd.vega.v5+json",
    schemaPrefix: "https://vega.github.io/schema/vega/v5",
  },
  VEGALITE1: {
    kind: "vega-lite",
    version: "1",
    mediaType: "application/vnd.vegalite.v1+json",
    schemaPrefix: "https://vega.github.io/schema/vega-lite/v1",
  },
  VEGALITE2: {
    kind: "vega-lite",
    version: "2",
    mediaType: "application/vnd.vegalite.v2+json",
    schemaPrefix: "https://vega.github.io/schema/vega-lite/v2",
  },
  VEGALITE3: {
    kind: "vega-lite",
    version: "3",
    mediaType: "application/vnd.vegalite.v3+json",
    schemaPrefix: "https://vega.github.io/schema/vega-lite/v3",
  },
};

export const Vega2 = makeTransform(VERSIONS.VEGA2, VEGA_EMBED_2);
export const Vega3 = makeTransform(VERSIONS.VEGA3, VEGA_EMBED_4);
export const Vega4 = makeTransform(VERSIONS.VEGA4, VEGA_EMBED_4);
export const Vega5 = makeTransform(VERSIONS.VEGA5, VEGA_EMBED_4);
export const VegaLite1 = makeTransform(VERSIONS.VEGALITE1, VEGA_EMBED_2);
export const VegaLite2 = makeTransform(VERSIONS.VEGALITE2, VEGA_EMBED_4);
export const VegaLite3 = makeTransform(VERSIONS.VEGALITE3, VEGA_EMBED_4);