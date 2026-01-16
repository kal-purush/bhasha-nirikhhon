import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { Essay } from '.'
import { QuestionTypeEnum } from '../../textSections'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'
import { WritingLevelEnum } from '../../students'

export const UpdateEssaysByQuestionInput = inputObjectType({
  name: 'UpdateEssaysByQuestionInput',
  definition(t) {
    t.string('originalQuestion', { required: true })
    t.string('newQuestion', { required: true })
    t.field('newQuestionType', { type: QuestionTypeEnum, required: true })
    t.field('newWritingLevel', { type: WritingLevelEnum, required: true })
  },
})

export const UpdateEssaysByQuestionPayload = objectType({
  name: 'UpdateEssaysByQuestionPayload',
  definition(t) {
    t.list.field('essays', { type: Essay })
  },
})

export const UpdateEssaysByQuestion = mutationField('updateEssaysByQuestion', {
  type: UpdateEssaysByQuestionPayload,
  args: { input: arg({ type: UpdateEssaysByQuestionInput, required: true }) },
  async resolve(
    _,
    {
      input: {
        originalQuestion,
        newQuestion,
        newQuestionType,
        newWritingLevel,
      },
    },
    { assignmentData }
  ) {
    const essayValidation: NexusGenRootTypes['Essay'][] = await assignmentData
      .find({ 'topic.question': originalQuestion })
      .toArray()
    const essayIds = essayValidation.map((essay) => essay._id) as string[]

    if (essayValidation.length > 0) {
      const updatedEssays: NexusGenRootTypes['Essay'][] = []

      await assignmentData.updateMany(
        { 'topic.question': originalQuestion },
        {
          $set: {
            'topic.question': newQuestion,
            'topic.questionType': newQuestionType,
            'topic.writingLevel': newWritingLevel,
          },
        }
      )
      for (const _id of essayIds) {
        const essay = await assignmentData.findOne({ _id: new ObjectId(_id) })
        updatedEssays.push(essay)
      }

      return { essays: updatedEssays }
    } else throw new Error('No essays for this Question')
  },
})