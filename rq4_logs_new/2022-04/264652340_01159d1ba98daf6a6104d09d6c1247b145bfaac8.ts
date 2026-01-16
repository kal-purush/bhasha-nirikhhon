import { initializeEnvironment } from "@tager/admin-services";

export function populateEnvironment() {
  initializeEnvironment({
    publicPath: process.env.VUE_APP_PUBLIC_PATH,
    appEnv: process.env.VUE_APP_ENV,
    apiUrl: process.env.VUE_APP_API_URL,
    accessToken: process.env.VUE_APP_ACCESS_TOKEN,
    websiteUrl: process.env.VUE_APP_WEBSITE_URL,
    websiteButtonUrl: process.env.VUE_APP_WEBSITE_BUTTON_URL,
    sentryDsn: process.env.VUE_APP_SENTRY_DSN,
    sentryEnv: process.env.VUE_APP_SENTRY_ENVIRONMENT,
    baseUrl: process.env.BASE_URL,
    nodeEnv: process.env.NODE_ENV,
    version: process.env.VUE_APP_VERSION,
  });
}