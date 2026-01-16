import { NotifyClientStubSuccess } from '../../stub/notify-client-stub-success';
import { INotifyClient } from '../../../domain/notify-client.interface';
import { RequestScheduler } from '../../../framework/request-scheduler';
import { IConfigAdapter } from '../../../framework/adapter/config/config-adapter.interface';
import { ITemplateIdProvider, TemplateIdProvider } from '../template-id-provider';
import { IStatusUpdater } from '../../../framework/status-updater';
import { IFaultProvider, FaultProvider } from '../fault-provider';
import { IPersonalisationProvider, PersonalisationProvider } from '../personalisation-provider';
import { StandardCarTestCATBSchema } from '@dvsa/mes-test-schema/categories/B';
import { ConfigAdapterMock } from '../../../framework/adapter/config/__mocks__/config-adapter.mock';
import { StatusUpdaterMock } from '../../../framework/__mocks__/status-updater.mock';
import { NextUploadBatchMock } from '../../../framework/__mocks__/next-upload-batch.mock';
import { NOTIFY_INTERFACE } from '../../../domain/interface.constants';
import { ProcessingStatus } from '../../../domain/submission-outcome.model';

describe('Test termination confirmation', () => {
  let configAdapter: IConfigAdapter;
  let templateIdProvider: ITemplateIdProvider;
  let statusUpdater: IStatusUpdater;
  let faultProvider: IFaultProvider;
  let personalisationProvider: IPersonalisationProvider;

  const totalNumberOfTests: number = 1;

  let testResults: StandardCarTestCATBSchema[];
  beforeEach(async () => {
    configAdapter = new ConfigAdapterMock();
    templateIdProvider = new TemplateIdProvider(configAdapter);
    statusUpdater = new StatusUpdaterMock();
    faultProvider = new FaultProvider();
    personalisationProvider = new PersonalisationProvider(faultProvider);

    spyOn(statusUpdater, 'updateStatus');

    testResults = await new NextUploadBatchMock().get(totalNumberOfTests);
  });

  describe('Terminated test sendEmail handling', () => {
    it('should not send an email when templateId is empty', async (done) => {
      const notifyClient: INotifyClient = new NotifyClientStubSuccess();
      testResults.forEach(result => result.activityCode = '11');
      const requestScheduler =
        new RequestScheduler(configAdapter, notifyClient, templateIdProvider, personalisationProvider, statusUpdater);

      spyOn(notifyClient, 'sendEmail');
      await requestScheduler.scheduleRequests(testResults);

      setTimeout(() => {
        expect(notifyClient.sendEmail).not.toHaveBeenCalled();
        expect(statusUpdater.updateStatus).toHaveBeenCalledWith({
          applicationReference: 12345672011,
          outcomePayload: {
            interface: NOTIFY_INTERFACE,
            state: ProcessingStatus.ACCEPTED,
            staff_number: '123456',
            retry_count: 0,
            error_message: null,
          },
        });
        done();
      },         1000);
    });

    it('should send an email for terminated activity code 4', async (done) => {
      const notifyClient: INotifyClient = new NotifyClientStubSuccess();
      testResults.forEach(result => result.activityCode = '4');
      const requestScheduler =
        new RequestScheduler(configAdapter, notifyClient, templateIdProvider, personalisationProvider, statusUpdater);

      spyOn(notifyClient, 'sendEmail');
      await requestScheduler.scheduleRequests(testResults);

      setTimeout(() => {
        expect(notifyClient.sendEmail).toHaveBeenCalled();
        expect(statusUpdater.updateStatus).toHaveBeenCalledWith({
          applicationReference: 12345672011,
          outcomePayload: {
            interface: NOTIFY_INTERFACE,
            state: ProcessingStatus.ACCEPTED,
            staff_number: '123456',
            retry_count: 0,
            error_message: null,
          },
        });
        done();
      },         1000);
    });

    it('should send an email for terminated activity code 5', async (done) => {
      const notifyClient: INotifyClient = new NotifyClientStubSuccess();
      testResults.forEach(result => result.activityCode = '5');
      const requestScheduler =
        new RequestScheduler(configAdapter, notifyClient, templateIdProvider, personalisationProvider, statusUpdater);

      spyOn(notifyClient, 'sendEmail');
      await requestScheduler.scheduleRequests(testResults);

      setTimeout(() => {
        expect(notifyClient.sendEmail).toHaveBeenCalled();
        expect(statusUpdater.updateStatus).toHaveBeenCalledWith({
          applicationReference: 12345672011,
          outcomePayload: {
            interface: NOTIFY_INTERFACE,
            state: ProcessingStatus.ACCEPTED,
            staff_number: '123456',
            retry_count: 0,
            error_message: null,
          },
        });
        done();
      },         1000);
    });
  });

  describe('Terminated test sendLetter handling', () => {
    it('should not send an email when templateId is empty', async (done) => {
      const notifyClient: INotifyClient = new NotifyClientStubSuccess();
      testResults.forEach(result => result.activityCode = '11');
      const requestScheduler =
        new RequestScheduler(configAdapter, notifyClient, templateIdProvider, personalisationProvider, statusUpdater);

      spyOn(notifyClient, 'sendLetter');
      await requestScheduler.scheduleRequests(testResults);

      setTimeout(() => {
        expect(notifyClient.sendLetter).not.toHaveBeenCalled();
        expect(statusUpdater.updateStatus).toHaveBeenCalledWith({
          applicationReference: 12345672011,
          outcomePayload: {
            interface: NOTIFY_INTERFACE,
            state: ProcessingStatus.ACCEPTED,
            staff_number: '123456',
            retry_count: 0,
            error_message: null,
          },
        });
        done();
      },         1000);
    });

    it('should send a letter for terminated activity code 4', async (done) => {
      const notifyClient: INotifyClient = new NotifyClientStubSuccess();
      testResults.forEach((result) => {
        result.activityCode = '4';
        result.communicationPreferences = {
          updatedEmail: '',
          communicationMethod: 'Post',
          conductedLanguage: 'English',
        };
      });
      const requestScheduler =
        new RequestScheduler(configAdapter, notifyClient, templateIdProvider, personalisationProvider, statusUpdater);

      spyOn(notifyClient, 'sendLetter');
      await requestScheduler.scheduleRequests(testResults);

      setTimeout(() => {
        expect(notifyClient.sendLetter).toHaveBeenCalled();
        expect(statusUpdater.updateStatus).toHaveBeenCalledWith({
          applicationReference: 12345672011,
          outcomePayload: {
            interface: NOTIFY_INTERFACE,
            state: ProcessingStatus.ACCEPTED,
            staff_number: '123456',
            retry_count: 0,
            error_message: null,
          },
        });
        done();
      },         1000);
    });

    it('should send a letter for terminated activity code 5', async (done) => {
      const notifyClient: INotifyClient = new NotifyClientStubSuccess();
      testResults.forEach((result) => {
        result.activityCode = '5';
        result.communicationPreferences = {
          updatedEmail: '',
          communicationMethod: 'Post',
          conductedLanguage: 'English',
        };
      });
      const requestScheduler =
        new RequestScheduler(configAdapter, notifyClient, templateIdProvider, personalisationProvider, statusUpdater);

      spyOn(notifyClient, 'sendLetter');
      await requestScheduler.scheduleRequests(testResults);

      setTimeout(() => {
        expect(notifyClient.sendLetter).toHaveBeenCalled();
        expect(statusUpdater.updateStatus).toHaveBeenCalledWith({
          applicationReference: 12345672011,
          outcomePayload: {
            interface: NOTIFY_INTERFACE,
            state: ProcessingStatus.ACCEPTED,
            staff_number: '123456',
            retry_count: 0,
            error_message: null,
          },
        });
        done();
      },         1000);
    });
  });
});