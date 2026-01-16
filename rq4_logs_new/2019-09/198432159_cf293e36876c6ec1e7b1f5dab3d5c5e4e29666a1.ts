import {Action, ActionCreator} from 'redux';
import {GenericObject} from '../../types/globals';

export const UPDATE_ASSESSMENT_DATA = 'UPDATE_ASSESSMENT_DATA';

export interface AssessmentActionUpdate extends Action<'UPDATE_ASSESSMENT_DATA'> {
  currentAssessment: GenericObject
}

export type PageDataAction = AssessmentActionUpdate;

export const updateAssessmentData: ActionCreator<AssessmentActionUpdate> = (currentAssessment : {}) => {
  return {
    type: UPDATE_ASSESSMENT_DATA,
    currentAssessment
  };
};