import {getClient} from './tokenUtil';
import {AutomationSheetID} from './constants';
import SheetWrapper from './SheetWrapper';

interface ILog {
  DateTime: string;
  Message: string;
}

const writeLog = async (message: string) => {
  const now = new Date();
  const log: ILog = {
    DateTime: `${now.getFullYear()}-${now.getMonth()}-${now.getDay()} ${now.getHours()}:${now.getMinutes()}:${now.getSeconds()}.${now.getMilliseconds()}`,
    Message: message,
  };

  try {
    const client = await getClient();

    const logSheetWrapper = new SheetWrapper(AutomationSheetID, client);

    logSheetWrapper.write([log], 'B', 'Logs');
    console.log(`[${log.DateTime}] ${log.Message}`);
  } catch (error) {
    console.error('Encountered error while logging', error);
  }
};

export {writeLog};