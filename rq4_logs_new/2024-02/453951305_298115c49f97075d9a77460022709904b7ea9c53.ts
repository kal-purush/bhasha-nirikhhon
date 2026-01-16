import { logger } from "@navikt/next-logger";
import { evaluateFlags, flagsClient, getDefinitions } from "@unleash/nextjs";

export interface IFeatureToggles {
  featureToggles: { [key: string]: boolean };
}

export async function getFeatureToggles() {
  const featureToggles = {
    arbeidsforholdIsEnabled: false,
  };

  try {
    const definitions = await getDefinitions();
    const { toggles } = evaluateFlags(definitions);
    const flags = flagsClient(toggles);

    featureToggles.arbeidsforholdIsEnabled = flags.isEnabled("dp-soknadsdialog-arbeidsforhold");

    return featureToggles;
  } catch (error) {
    logger.error(`Unleash error: ${error}`);

    return featureToggles;
  }
}