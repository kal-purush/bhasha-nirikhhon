import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { Essay } from './essays'
import { ObjectId } from 'mongodb'
import { MarkingPeriodEnum } from '../../general'

export const UpdateEssaysByLessonInput = inputObjectType({
  name: 'UpdateEssaysByLessonInput',
  definition(t) {
    t.id('lessonId', { required: true })
    t.field('markingPeriod', { type: MarkingPeriodEnum, required: true })
  },
})

export const UpdateEssaysByLessonPayload = objectType({
  name: 'UpdateEssaysByLessonPayload',
  definition(t) {
    t.list.field('essays', { type: Essay })
  },
})

export const UpdateEssaysByLesson = mutationField('updateEssaysByLesson', {
  type: UpdateEssaysByLessonPayload,
  description: 'only useful for changing marking period',
  args: { input: arg({ type: UpdateEssaysByLessonInput, required: true }) },
  async resolve(_, { input: { lessonId, markingPeriod } }, { assignmentData }) {
    const essayValidation = await assignmentData
      .find({
        associatedLessonId: lessonId,
      })
      .toArray()

    if (essayValidation.length > 0) {
      await assignmentData.updateMany(
        {
          associatedLessonId: lessonId,
        },
        { $set: { markingPeriod: markingPeriod } }
      )
      const essays = await assignmentData
        .find({
          associatedLessonId: lessonId,
        })
        .toArray()
      return { essays }
    } else throw new Error('No essays for that lesson')
  },
})