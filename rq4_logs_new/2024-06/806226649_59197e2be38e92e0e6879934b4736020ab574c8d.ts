import {TestBed, waitForAsync} from "@angular/core/testing";
import {HttpClientTestingModule, HttpTestingController} from "@angular/common/http/testing";
import {MeasurementService} from "./measurement.service";
import {ToastService} from "../../../shared/components/toast/toast.service";
import {ErrorResponseService} from "../../../shared/services/error-response.service";
import {AppSettingsState} from "../../../core/app-settings/+state/app-settings.state";
import {AuthState} from "../../../core/auth/+state/auth.state";
import {latestMeasurementsMock} from "../../../../../mock/measurement.mock";
import {signal} from "@angular/core";
import {mdiAlert} from "@mdi/js";
import {temperatureMeasurementsHistory} from "../../../../../mock/temperatureMeasurementHistory.mock";
import {DateTimeUtil} from "../../../shared/util/date.util";

describe('MeasurmentService Test', () => {
  let service: MeasurementService;
  let httpClient: HttpTestingController;
  let toastService: ToastService;
  let errorStatusService: ErrorResponseService;
  let appSettingsState: AppSettingsState;
  let authState: any;

  beforeEach(waitForAsync(() => {
    authState = {
      user: signal({
        id: 1
      })
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [{provide: AuthState, useValue: authState}]
    });

    service = TestBed.inject(MeasurementService);
    httpClient = TestBed.inject(HttpTestingController);
    toastService = TestBed.inject(ToastService);
    errorStatusService = TestBed.inject(ErrorResponseService);
    appSettingsState  = TestBed.inject(AppSettingsState);
  }));

  afterEach(() => {
    httpClient.verify();
  });


  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should fetch latest measurements without error', async () => {
    // given
    const errorSpy = jest.spyOn(errorStatusService, 'getHttpErrorResponseTextByStatus');
    const toastSpy = jest.spyOn(toastService, 'show');

    // when
    const promise = service.getLatestMeasurements();
    const req = httpClient.expectOne(`${appSettingsState.baseUrl()}/measurements/user/1`)
    expect(req.request.method).toBe("GET");
    req.flush(latestMeasurementsMock, {status: 200, statusText: "OK"});

    //then
    const measurements = await promise;
    expect(measurements).toEqual(latestMeasurementsMock);
    expect(errorSpy).not.toHaveBeenCalled();
    expect(toastSpy).not.toHaveBeenCalled();
  });

  it('should fetch latest measurements and handle error', async () => {
    // given
    const errorSpy = jest.spyOn(errorStatusService, 'getHttpErrorResponseTextByStatus');
    const toastSpy = jest.spyOn(toastService, 'show');
    const expectedToast = {classname: "bg-danger text-light", header: 'Could not load latest measurements',
      id: "dashboard-error",
      body: `FORBIDDEN`, icon: mdiAlert, iconColor: "white"};
    // when
    const promise = service.getLatestMeasurements();
    const req = httpClient.expectOne(`${appSettingsState.baseUrl()}/measurements/user/1`)
    expect(req.request.method).toBe("GET");
    req.flush([], {status: 403, statusText: "FORBIDDEN"});

    //then
    const measurements = await promise;
    expect(measurements).toEqual([]);
    expect(errorSpy).toHaveBeenCalled();
    expect(toastSpy).toHaveBeenCalledWith(expectedToast);
  });

  it('should fetch latest measurement history without error', async () => {
    // given
    const errorSpy = jest.spyOn(errorStatusService, 'getHttpErrorResponseTextByStatus');
    const toastSpy = jest.spyOn(toastService, 'show');
    const id = 1;
    const from = DateTimeUtil.setDateToStartOfDay(new Date(2024, 5, 28, 0, 0));
    const to = DateTimeUtil.setDateToEndOfDay(new Date(2024, 5, 28, 23, 59));
    // when
    const promise = service.getMeasurementHistory(id, from, to, false);
    const req = httpClient.expectOne(`${appSettingsState.baseUrl()}/measurements/sensor/1?from=${from.toISOString()}&to=${to.toISOString()}`);
    expect(req.request.method).toBe("GET");
    req.flush(temperatureMeasurementsHistory, {status: 200, statusText: "OK"});

    //then
    const history = await promise;
    expect(history).toEqual(temperatureMeasurementsHistory);
    expect(errorSpy).not.toHaveBeenCalled();
    expect(toastSpy).not.toHaveBeenCalled();
  });

  it('should fetch latest measurement history for base without error', async () => {
    // given
    const errorSpy = jest.spyOn(errorStatusService, 'getHttpErrorResponseTextByStatus');
    const toastSpy = jest.spyOn(toastService, 'show');
    const id = 1;
    const from = DateTimeUtil.setDateToStartOfDay(new Date(2024, 5, 28, 0, 0));
    const to = DateTimeUtil.setDateToEndOfDay(new Date(2024, 5, 28, 23, 59));
    // when
    const promise = service.getMeasurementHistory(id, from, to, true);
    const req = httpClient.expectOne(`${appSettingsState.baseUrl()}/measurements/base/1?from=${from.toISOString()}&to=${to.toISOString()}`);
    expect(req.request.method).toBe("GET");
    req.flush(temperatureMeasurementsHistory, {status: 200, statusText: "OK"});

    //then
    const history = await promise;
    expect(history).toEqual(temperatureMeasurementsHistory);
    expect(errorSpy).not.toHaveBeenCalled();
    expect(toastSpy).not.toHaveBeenCalled();
  });

  it('should fetch latest measurement history and handle error', async () => {
    // given
    const errorSpy = jest.spyOn(errorStatusService, 'getHttpErrorResponseTextByStatus');
    const toastSpy = jest.spyOn(toastService, 'show');
    const id = 1;
    const from = DateTimeUtil.setDateToStartOfDay(new Date(2024, 5, 28, 0, 0));
    const to = DateTimeUtil.setDateToEndOfDay(new Date(2024, 5, 28, 23, 59));
    const expectedToast = {classname: "bg-danger text-light", header: 'Could not load measurements history',
      id: 'dashboard-error',
      body: `FORBIDDEN`, icon: mdiAlert, iconColor: "white"};

    // when
    const promise = service.getMeasurementHistory(id, from, to, false);
    const req = httpClient.expectOne(`${appSettingsState.baseUrl()}/measurements/sensor/1?from=${from.toISOString()}&to=${to.toISOString()}`);
    expect(req.request.method).toBe("GET");
    req.flush(null, {status: 403, statusText: "FORBIDDEN"});

    //then
    await expect(promise).rejects.toEqual(new Error("Could not load history data"));
    expect(errorSpy).toHaveBeenCalled();
    expect(toastSpy).toHaveBeenCalledWith(expectedToast);
  });




})