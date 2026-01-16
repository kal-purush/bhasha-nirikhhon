import {defineNuxtPlugin} from '#app';
import * as Sentry from '@sentry/vue';
import {
  HttpClient as HttpClientIntegration,
  ReportingObserver as ReportingObserverIntegration,
} from '@sentry/integrations'
import type {Router} from 'vue-router'

export default defineNuxtPlugin((nuxtApp) => {
  Sentry.init({
    dsn: "https://463b5402b40274cd852c4f11cc14e298@o4506422635331584.ingest.sentry.io/4506422636969984",
    integrations: [
      new Sentry.BrowserTracing({
        routingInstrumentation: Sentry.vueRouterInstrumentation(nuxtApp.$router as Router, {
          routeLabel: 'path',
        }),
      }),
      new HttpClientIntegration(),
      new ReportingObserverIntegration(),
      new Sentry.Replay(),
    ],
    tracesSampleRate: 1.0,
    tracePropagationTargets: ["localhost", /^https:\/\/yourserver\.io\/api/],
    replaysSessionSampleRate: 0.1,
    replaysOnErrorSampleRate: 1.0,
  });
});