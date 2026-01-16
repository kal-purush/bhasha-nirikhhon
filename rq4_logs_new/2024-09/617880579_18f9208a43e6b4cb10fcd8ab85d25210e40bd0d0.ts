import { onlyOfficeForgottenFilesCheckPeriodMS } from '@/config';
import { PolledThingieValue } from '@/lib/polled-thingie-value';

import apiService from './api.service';
import onlyofficeService from './onlyoffice.service';
import driveService from './drive.service';
import logger from '@/lib/logger';

/**
 * Periodically poll the Only Office document server for forgotten
 * files and try to upload to Twake Drive or get rid if unknown.
 */
class ForgottenProcessor {
  private readonly forgottenFilesPoller: PolledThingieValue<number>;
  private lastStart = 0;

  constructor() {
    let skippedFirst = false;
    this.forgottenFilesPoller = new PolledThingieValue(
      'Process forgotten files in OO',
      async () => (skippedFirst ? await this.processForgottenFiles() : ((skippedFirst = true), -1)),
      onlyOfficeForgottenFilesCheckPeriodMS,
    );
  }

  /** Get the number of seconds since the last time this process started */
  public getLastStartTimeAgoS() {
    return ~~((new Date().getTime() - this.lastStart) / 1000);
  }

  public makeSureItsLoaded() {
    // The point of this is to ensure this file is imported,
    // which is needed for side-effect of starting this timer
    return true;
  }

  private async processForgottenFiles() {
    this.lastStart = new Date().getTime();
    if (!(await apiService.hasToken())) return -1;
    return await onlyofficeService.processForgotten(async (key, url) => {
      try {
        await driveService.endEditing(key, url);
        return true;
      } catch (error) {
        logger.error(`Error processing forgotten file by key ${JSON.stringify(key)}: ${error}`, { key, url, error });
        // Can't do much about it here, hope it goes in retry, but don't
        // throw to keep processing
        //TODO: Maybe make a date string, compare to key, if old enough, delete...
        // this logic should probably in Twake Drive backend though
      }
      return false;
    });
  }
}

export default new ForgottenProcessor();