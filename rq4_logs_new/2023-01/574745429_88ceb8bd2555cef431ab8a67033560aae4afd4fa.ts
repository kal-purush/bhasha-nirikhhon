import type { CSSProperties } from 'react';
import { match } from 'ts-pattern';

export enum LearningLevel {
  Beginner = 'BEGINNER',
  Intermediate = 'INTERMEDIATE',
  Advanced = 'ADVANCED',
  Expert = 'EXPERT',
}

export const getLearningLevelBgColor = (learningLevel: LearningLevel) =>
  match<LearningLevel, CSSProperties>(learningLevel)
    .with(LearningLevel.Beginner, () => ({ backgroundColor: '#76ef81' }))
    .with(LearningLevel.Intermediate, () => ({ backgroundColor: '#3abff8' }))
    .with(LearningLevel.Advanced, () => ({ backgroundColor: '#fef08a' }))
    .with(LearningLevel.Expert, () => ({ backgroundColor: '#c39af1' }))
    .exhaustive();