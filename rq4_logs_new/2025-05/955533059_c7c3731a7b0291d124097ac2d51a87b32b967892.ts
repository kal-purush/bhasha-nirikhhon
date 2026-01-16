import { QuestionType } from '../../components/questions/utils';
import { ProjectType } from '@/lib/utils/types/Projects';
import { TrackerType } from './types/Tracker';
/**
 * Current data structure:
 * All questions by id
 * trackers have an ordered list of question ids
 */

export const AvailableQuestions = {
  label: {
    label: 'Title',
    type: QuestionType.input,
  },
  startDate: {
    label: 'Start date',
    type: QuestionType.date,
  },
  endDate: {
    label: 'Finish date',
    type: QuestionType.date,
  },
  notes: {
    label: 'Notes',
    type: QuestionType.textarea,
  },
  pattern: {
    label: 'Pattern',
    type: QuestionType.input,
  },
  currentRow: {
    label: 'Current row',
    type: QuestionType.number,
  },
  hook: {
    label: 'Hook size',
    type: QuestionType.slider,
    defaultValue: 4,
    minValue: 0.25,
    maxValue: 10,
    step: 0.25,
    allowManual: true,
  },
  yarn: {
    label: 'Yarn details',
    type: QuestionType.input,
  },
} as const;
export type AvailableQuestions = keyof typeof AvailableQuestions;

export type TrackerDef = {
  questions: Array<AvailableQuestions>;
  label?: string;
  icon?: string;
};
export const Trackers: Record<
  ProjectType | Exclude<TrackerType, 'project'>,
  TrackerDef
> = {
  [ProjectType.crochet]: {
    questions: [
      'label',
      'pattern',
      'currentRow',
      'hook',
      'yarn',
      'startDate',
      'endDate',
      'notes',
    ],
  },
  [ProjectType.knit]: {
    questions: [
      'label',
      'pattern',
      'currentRow',
      'hook',
      'yarn',
      'startDate',
      'endDate',
      'notes',
    ],
  },
  [TrackerType.medication]: {
    questions: [],
  },
};