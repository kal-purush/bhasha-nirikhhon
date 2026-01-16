// import { getJournal } from '../../framework/aws/DynamoJournalRepository';
// import { decompressJournal } from './journal-decompressor';
// import * as logger from '../../../../common/application/utils/logger';
// import { JournalNotFoundError } from '../../domain/errors/journal-not-found-error';
// import { JournalDecompressionError } from '../../domain/errors/journal-decompression-error';
// import { JournalRecord } from '../../domain/JournalRecord';

// /**
//  * Finds a journal with a specified staffNumber.
//  * Throws a JournalNotFoundError if it the repo could not find one.
//  * Throws a JournalDecompressionError if decompression fails
//  * @param staffNumber the staff number of the journal to find
//  */
// export async function findJournal(
//   staffNumber: string,
//   modifiedSinceTimestamp: number | null,
// ): Promise<ExaminerWorkSchedule | null> {
//   const journalRecord = await getJournal(staffNumber);
//   if (!journalRecord) {
//     throw new JournalNotFoundError();
//   }
//   if (journalNotModifiedSince(journalRecord, modifiedSinceTimestamp)) {
//     return null;
//   }

//   try {
//     return decompressJournal(journalRecord.journal);
//   } catch (error) {
//     logger.error(error);
//     throw new JournalDecompressionError();
//   }
// }

// const journalNotModifiedSince = (journalRecord: JournalRecord, modifiedSinceTimestamp: number | null): boolean => {
//   return !!modifiedSinceTimestamp && journalRecord.lastUpdatedAt <= modifiedSinceTimestamp;
// };