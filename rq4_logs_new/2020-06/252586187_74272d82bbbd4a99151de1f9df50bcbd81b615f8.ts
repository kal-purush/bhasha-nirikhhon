import { objectType, inputObjectType } from '@nexus/schema'
import { AnswerTypes } from './answerTypes'
import { QuestionTypeEnum } from '../../../textSections/textSection'

export const AcademicOrganizer = objectType({
  name: 'AcademicOrganizer',
  definition(t) {
    t.typeName
    t.field('questionType', { type: QuestionTypeEnum, nullable: true })
    t.field('answerType', { type: AnswerTypes, nullable: true })
    t.field('academicSentenceStructure', { type: AcademicSentenceStructure })
    t.string('restatement')
    t.string('conclusion')
  },
})

export const AcademicSentenceStructure = objectType({
  name: 'AcademicSentenceStructure',
  definition(t) {
    t.string('subject')
    t.string('verb')
    t.string('object', { nullable: true })
  },
})

export const AcademicSentenceStructureInput = inputObjectType({
  name: 'AcademicSentenceStructureInput',
  definition(t) {
    t.string('subject', { required: true })
    t.string('verb', { required: true })
    t.string('object')
  },
})