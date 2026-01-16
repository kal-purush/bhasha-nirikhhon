import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { Essay } from './essays'
import { ObjectId } from 'mongodb'

export const UpdateEssayByLessonInput = inputObjectType({
  name: 'UpdateEssayByLessonInput',
  definition(t) {
    t.id('lessonId', { required: true })
    t.string('markingPeriod', { required: true })
  },
})

export const UpdateEssayByLessonPayload = objectType({
  name: 'UpdateEssayByLessonPayload',
  definition(t) {
    t.list.field('essays', { type: Essay })
  },
})

export const UpdateEssayByLesson = mutationField('updateEssayByLesson', {
  type: UpdateEssayByLessonPayload,
  args: { input: arg({ type: UpdateEssayByLessonInput, required: true }) },
  async resolve(_, { input: { lessonId, markingPeriod } }, { assignmentData }) {
    const essayValidation = await assignmentData.find({
      associatedLessonId: new ObjectId(lessonId),
    })
    if (essayValidation.length > 0) {
      await assignmentData.updateOne(
        {
          associatedLessonId: new ObjectId(lessonId),
        },
        { $set: markingPeriod }
      )
      const essays = await assignmentData
        .find({
          associatedLessonId: new ObjectId(lessonId),
        })
        .toArray()
      return { essays }
    } else throw new Error('No essays for that lesson')
  },
})