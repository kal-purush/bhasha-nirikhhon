import { Logger } from '../../services/logger';
import { STORAGE_NAMES, STORAGE_TYPES } from '../storage-utils/storage-utils.types';
import { NoCodeAssessment } from '../no-code-assessment/no-code-assessment.types';

import { StrigoAssessmentsStorage } from './assessments-storage.types';

export function getAssessmentsStorageData(): StrigoAssessmentsStorage {
  try {
    return JSON.parse(window[STORAGE_TYPES.LOCAL_STORAGE].getItem(STORAGE_NAMES.STRIGO_ASSESSMENTS));
  } catch (error) {
    Logger.error('Get assessments storage error', { error });

    return null;
  }
}

export function init(): StrigoAssessmentsStorage {
  try {
    const currentAssessmentsStorage = getAssessmentsStorageData();

    if (currentAssessmentsStorage) {
      Logger.debug('Assessments storage already exists');

      return currentAssessmentsStorage;
    }

    const strigoAssessments = { assessments: [] };
    window[STORAGE_TYPES.LOCAL_STORAGE].setItem(STORAGE_NAMES.STRIGO_ASSESSMENTS, JSON.stringify(strigoAssessments));

    return strigoAssessments;
  } catch (error) {
    Logger.error('Init assessments storage error', { error });

    return null;
  }
}

export function setup(initialStorage?: NoCodeAssessment[]): StrigoAssessmentsStorage {
  try {
    const strigoAssessments = initialStorage ? { assessments: [...initialStorage] } : { assessments: [] };
    window[STORAGE_TYPES.LOCAL_STORAGE].setItem(STORAGE_NAMES.STRIGO_ASSESSMENTS, JSON.stringify(strigoAssessments));

    return strigoAssessments;
  } catch (error) {
    Logger.error('Assessments storage setup error', { error });

    return null;
  }
}

export function clearAssessmentStorage(): void {
  try {
    window[STORAGE_TYPES.LOCAL_STORAGE].removeItem(STORAGE_NAMES.STRIGO_ASSESSMENTS);
  } catch (error) {
    Logger.error('Clear assessments storage error', { error });
  }
}