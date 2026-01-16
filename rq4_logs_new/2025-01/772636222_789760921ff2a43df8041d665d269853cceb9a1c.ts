import { getBuildingBlockSDK, getM2MTokenFn } from "@ogcio/building-blocks-sdk";
import { headers } from "next/headers";
import { validateEnv } from "../utils/env";

const env = validateEnv<{
  ANALYTICS_URL: string;
  M2M_ANALYTICS_ORGANIZATION_ID: string;
  M2M_ANALYTICS_APP_ID: string;
  M2M_ANALYTICS_APP_SECRET: string;
  LOGTO_ENDPOINT: string;
}>({
  ANALYTICS_URL: process.env.ANALYTICS_URL,
  M2M_ANALYTICS_ORGANIZATION_ID: process.env.M2M_ANALYTICS_ORGANIZATION_ID,
  M2M_ANALYTICS_APP_ID: process.env.M2M_ANALYTICS_APP_ID,
  M2M_ANALYTICS_APP_SECRET: process.env.M2M_ANALYTICS_APP_SECRET,
  LOGTO_ENDPOINT: process.env.LOGTO_ENDPOINT,
});

export const analyticsConfig = {
  baseUrl: env.ANALYTICS_URL,
  trackingWebsiteId: process.env.ANALYTICS_WEBSITE_ID,
  organizationId: env.M2M_ANALYTICS_ORGANIZATION_ID,
  dryRun: !!process.env.NEXT_PUBLIC_ANALYTICS_DRY_RUN,
}

export const m2mAnalyticsConfig = {
  applicationId: env.M2M_ANALYTICS_APP_ID,
  applicationSecret: env.M2M_ANALYTICS_APP_SECRET,
  logtoOidcEndpoint: env.LOGTO_ENDPOINT?.endsWith("/")
    ? [env.LOGTO_ENDPOINT, "oidc"].join("")
    : [env.LOGTO_ENDPOINT, "oidc"].join("/"),
  organizationId: env.M2M_ANALYTICS_ORGANIZATION_ID,
}

export const getSDKs = () => getBuildingBlockSDK({
  services: {
    analytics: analyticsConfig
  },
  getTokenFn: async (serviceName: string) => invokeTokenApi(serviceName)
});

const invokeTokenApi = async (serviceName: string): Promise<string> => {
  let token = "";

  switch (serviceName) {
    case 'journey':
      token = await getAccessToken("/api/token");
      break;
    case 'payments':
      token = await getAccessToken("/api/payments-token");
      break;
    case 'messaging':
      token = await getAccessToken("/api/messaging-token");
      break;
    case 'analytics':
      token = await getM2MTokenFn({
        services: {
          analytics: {
            getOrganizationTokenParams: m2mAnalyticsConfig,
          },
        },
      })(serviceName);
  }

  return token;
};

const getAccessToken = async (path: string) => {
  const cookieHeader = headers().get("cookie") as string;

  const res = await fetch(
    new URL(
      path,
      process.env.NEXT_PUBLIC_JOURNEY_SERVICE_ENTRY_POINT as string,
    ),
    { headers: { cookie: cookieHeader } },
  );
  const { token } = await res.json();
  return token;
}