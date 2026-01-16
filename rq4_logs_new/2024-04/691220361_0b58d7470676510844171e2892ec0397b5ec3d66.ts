import { logging } from '../logging';
import { serverEnv } from '../serverEnv';

const transactionLoggingThreshold = serverEnv.TRANSACTIONLOG_TIME_MS;
const enableTransactionLogging = serverEnv.TRANSACTIONLOG_ENABLE;
const enableTransactionStartLogging = serverEnv.TRANSACTIONLOG_ENABLESTART;

export const tLogger = async <T>(title: string, query: Promise<T>) => {
	if (!enableTransactionLogging) return query;
	const start = Date.now();
	if (enableTransactionStartLogging) {
		logging.info(`Transaction "${title}" started`);
	}
	try {
		const result = await query;
		const duration = Date.now() - start;
		if (duration > transactionLoggingThreshold)
			logging.info(`Transaction "${title}" took ${duration}ms`);
		return result;
	} catch (err) {
		const duration = Date.now() - start;
		logging.error(`Transaction "${title}" failed after ${duration}ms`, err);
		throw err;
	}
};