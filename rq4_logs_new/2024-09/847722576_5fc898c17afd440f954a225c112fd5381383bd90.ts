import * as Sentry from "@sentry/nextjs";

Sentry.init({
  dsn: "https://2600b58ce7ace2e64953fbe4548e9019@o4508018046861312.ingest.us.sentry.io/4508018050138112",

  integrations: [Sentry.replayIntegration(),],
  // Session Replay
  replaysSessionSampleRate: 0.1, // This sets the sample rate at 10%. You may want to change it to 100% while in development and then sample at a lower rate in production.
  replaysOnErrorSampleRate: 1.0, // If you're not already sampling the entire session, change the sample rate to 100% when sampling sessions where errors occur.
});