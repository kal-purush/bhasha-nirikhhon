import vegaEmbedV2 from "@nteract/vega-embed-v2";
import { promisify } from "util";
import vegaEmbedV4 from "vega-embed";
import { MEDIA_TYPES, VegaMediaType } from "./mime";

/** Call the external library to do the embedding. */
export async function doEmbedding(
  anchor: HTMLElement,
  mediaType: VegaMediaType,
  spec: Readonly<{}>,
): Promise<any> {
  const version = MEDIA_TYPES[mediaType];
  const options = { actions: false, mode: version.kind };

  if (version.vegaLevel <= 2) {
    await promisify(vegaEmbedV2)(anchor, {...options, spec});
  }
  else {
    await vegaEmbedV4(anchor, deepThaw(spec), options);
  }
}

function deepThaw(spec: Readonly<{}>): {} {
  return JSON.parse(JSON.stringify(spec));
}