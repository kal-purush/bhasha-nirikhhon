import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { NotificationService } from './notification.service';
import { ToastService } from '../../../shared/components/toast/toast.service';
import { ErrorResponseService } from '../../../shared/services/error-response.service';
import { environment } from '../../../../environments/environment';
import { mdiAlert, mdiCheck } from '@mdi/js';

describe('NotificationService', () => {
  let service: NotificationService;
  let httpMock: HttpTestingController;
  let toastService: ToastService;
  let errorResponseService: ErrorResponseService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [NotificationService, ToastService, ErrorResponseService]
    }).compileComponents();

    service = TestBed.inject(NotificationService);
    httpMock = TestBed.inject(HttpTestingController);
    toastService = TestBed.inject(ToastService);
    errorResponseService = TestBed.inject(ErrorResponseService);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should return notifications on success', async () => {
    const mockNotifications = [{ id: 1, message: 'Test Notification' }];
    const userId = 1;

    const promise = service.getNotifications(userId);

    const req = httpMock.expectOne(`${environment.baseUrl}/notifications/user/${userId}`);
    expect(req.request.method).toBe('GET');
    req.flush(mockNotifications);

    const notifications = await promise;
    expect(notifications).toEqual(mockNotifications);
  });

  it('should show error toast on failure', async () => {
    const userId = 1;
    jest.spyOn(toastService, 'show');

    const promise = service.getNotifications(userId);

    const req = httpMock.expectOne(`${environment.baseUrl}/notifications/user/${userId}`);
    expect(req.request.method).toBe('GET');
    req.flush('Error', { status: 500, statusText: 'Server Error' });

    const notifications = await promise;
    expect(notifications).toEqual([]);
    expect(toastService.show).toHaveBeenCalledWith({
      classname: 'bg-danger text-light',
      header: 'Could not load notifications',
      body: '500 INTERNAL SERVER ERROR',
      icon: mdiAlert,
      iconColor: 'white'
    });
  });

  it('should show success toast on delete success', async () => {
    const userId = 1;
    jest.spyOn(toastService, 'show');

    const promise = service.sendDeleteNotification(userId);

    const req = httpMock.expectOne(`${environment.baseUrl}/notifications/user/${userId}`);
    expect(req.request.method).toBe('DELETE');
    req.flush({});

    await promise;
    expect(toastService.show).toHaveBeenCalledWith({
      classname: 'bg-success text-light',
      header: '',
      body: 'Deleted all notifications successfully',
      icon: mdiCheck,
      iconColor: 'white'
    });
  });

  it('should show error toast on delete failure', async () => {
    const userId = 1;
    jest.spyOn(toastService, 'show');
    jest.spyOn(errorResponseService, 'getHttpErrorResponseTextByStatus').mockReturnValue('Error');

    const promise = service.sendDeleteNotification(userId);

    const req = httpMock.expectOne(`${environment.baseUrl}/notifications/user/${userId}`);
    expect(req.request.method).toBe('DELETE');
    req.flush('Error', { status: 500, statusText: 'Server Error' });

    await promise.catch(e => e); // Handle the rejection to avoid unhandled promise rejection

    expect(toastService.show).toHaveBeenCalledWith({
      classname: 'bg-danger text-light',
      header: 'Could not delete notifications',
      body: '500 Error',
      icon: mdiAlert,
      iconColor: 'white'
    });
  });
});