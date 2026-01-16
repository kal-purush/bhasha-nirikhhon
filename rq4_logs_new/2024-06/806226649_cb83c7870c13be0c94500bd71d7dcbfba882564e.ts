import {TestBed, waitForAsync} from "@angular/core/testing";
import {AppSettingsService} from "../service/app-settings.service";
import {AppSettingsState} from "./app-settings.state";
import {AppSettings, SettingsType} from "../model/app-settings.model";


describe('AppSettings Test', () => {
  let appSettingsService: AppSettingsService;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      providers: [AppSettingsService],
    });

    appSettingsService = TestBed.inject(AppSettingsService);
  }))

  it('should load app settings and patch state', () => {
    // given
    const appSettingsState = TestBed.inject(AppSettingsState);
    const customSettings: AppSettings = {
      api: "/api/app",
      darkMode: false,
      dashboardRefreshInterval: 30000,
      host: "any-host",
      https: true,
      port: 443,
      tokenRefreshInterval: 120000,
      type: SettingsType.CUSTOM
    };
    const serviceSpy = jest.spyOn(appSettingsService, 'loadAppSettings')
      .mockReturnValue(customSettings);

    // when
    appSettingsState.loadAppSettings();

    // then
    expect(serviceSpy).toHaveBeenCalled();
    expect(appSettingsState.appSettings().type).toEqual(SettingsType.CUSTOM);
    expect(appSettingsState.appSettings().host).toEqual("any-host");
    expect(appSettingsState.appSettings().darkMode).toBeFalsy();
    expect(appSettingsState.appSettings().dashboardRefreshInterval).toEqual(30000);
    expect(appSettingsState.appSettings().https).toBeTruthy();
    expect(appSettingsState.appSettings().tokenRefreshInterval).toEqual(120000);
    expect(appSettingsState.baseUrl()).toEqual('https://any-host:443/api/app');
  });

  it('should save app settings and patch state', () => {
    // given
    const appSettingsState = TestBed.inject(AppSettingsState);
    const customSettings: AppSettings = {
      api: "/api/app",
      darkMode: true,
      dashboardRefreshInterval: 30000,
      host: "another-host",
      https: true,
      port: 443,
      tokenRefreshInterval: 120000,
      type: SettingsType.CUSTOM
    };
    const serviceSpy = jest.spyOn(appSettingsService, 'saveSettingsToLocalStorage');

    // when
    appSettingsState.saveAppSettings(customSettings);

    // then
    expect(serviceSpy).toHaveBeenCalledWith(customSettings);
    expect(appSettingsState.appSettings().type).toEqual(SettingsType.CUSTOM);
    expect(appSettingsState.appSettings().host).toEqual("another-host");
    expect(appSettingsState.appSettings().darkMode).toBeTruthy();
    expect(appSettingsState.appSettings().dashboardRefreshInterval).toEqual(30000);
    expect(appSettingsState.appSettings().https).toBeTruthy();
    expect(appSettingsState.appSettings().tokenRefreshInterval).toEqual(120000);
  });

  it('should restore app settings', () => {
    // given
    const appSettingsState = TestBed.inject(AppSettingsState);
    const customSettings: AppSettings = {
      api: "/api/app",
      darkMode: true,
      dashboardRefreshInterval: 30000,
      host: "another-host",
      https: true,
      port: 443,
      tokenRefreshInterval: 120000,
      type: SettingsType.CUSTOM
    };
    appSettingsState.saveAppSettings(customSettings);
    const serviceSpy = jest.spyOn(appSettingsService, 'deleteLocalStoredSettings');
    expect(appSettingsState.appSettings().type).toEqual(SettingsType.CUSTOM);
    expect(appSettingsState.baseUrl()).toEqual('https://another-host:443/api/app')

    // when
    appSettingsState.restoreDefaultSettings();

    // then
    expect(serviceSpy).toHaveBeenCalled();
    expect(appSettingsState.appSettings().type).toEqual(SettingsType.INITIAL);
    expect(appSettingsState.baseUrl()).toEqual('http://localhost:3001/api/app');
  });



});