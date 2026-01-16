import { AppSettings, SettingsType } from '../model/app-settings.model';
import { patchState, signalStore, withHooks, withMethods, withState } from '@ngrx/signals';

export const INITIAL_SETTINGS: AppSettings = {
  serverBaseUrl: 'http://localhost:8080/api/app',
  tokenRefreshMillis: 1000 * 60 * 4.5,
  dashboardRefreshMillis: 1000 * 60,
  darkMode: false,
  type: SettingsType.INITIAL
}

export const AppSettingsState = signalStore(
  {providedIn: 'root'},
  withState({
    appSettings: INITIAL_SETTINGS,
  }),
  withMethods((store) => {
    return {
      async loadAppSettings() {
        const appSettings = localStorage.getItem('app_settings') ?? JSON.stringify(INITIAL_SETTINGS);
        const deserializedSettings = JSON.parse(appSettings);
        patchState(store, {appSettings: deserializedSettings});
      }
    }
  }),
  withHooks({onInit(store) {
      store.loadAppSettings();
    }})
);

export type AppSettingsState = InstanceType<typeof AppSettingsState>;