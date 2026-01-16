import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { RubricEntry, RubricSectionEnum } from './rubricEntry'
import { WritingLevelEnum } from '../../../students/progress-metrics/writingMetrics'
import { NexusGenRootTypes } from '../../../../teachers-aid-typegen'

export const CreateRubricEntryInput = inputObjectType({
  name: 'CreateRubricEntryInput',
  definition(t) {
    t.string('entry', { required: true })
    t.int('score', { required: true })
    t.field('rubricSection', { type: RubricSectionEnum, required: true })
    t.list.field('rubricCategories', { type: WritingLevelEnum, required: true })
  },
})

export const CreateRubricEntryPayload = objectType({
  name: 'CreateRubricEntryPayload',
  definition(t) {
    t.field('rubricEntry', { type: RubricEntry })
  },
})

export const CreateRubricEntry = mutationField('createRubricEntry', {
  type: CreateRubricEntryPayload,
  args: { input: arg({ type: CreateRubricEntryInput, required: true }) },
  async resolve(
    _,
    { input: { entry, score, rubricSection, rubricCategories } },
    { rubricData }
  ) {
    const rubricEntry: NexusGenRootTypes['RubricEntry'] = {
      entry,
      score,
      rubricSection,
      rubricCategories,
    }
    const insertedId = await rubricData.insertOne(rubricEntry)
    rubricEntry._id = insertedId
    return { rubricEntry }
  },
})