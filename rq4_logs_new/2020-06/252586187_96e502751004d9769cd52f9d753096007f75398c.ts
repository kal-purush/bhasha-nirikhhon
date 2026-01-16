import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'
import { Essay } from '.'
import { TopicInput } from './essays'

export const UpdateEssayByStudentInput = inputObjectType({
  name: 'UpdateEssayByStudentInput',
  definition(t) {
    t.id('studentId', { required: true })
    t.string('section', { required: true })
    t.field('topic', { type: TopicInput })
    t.date('dueDate', { required: true })
  },
})

export const UpdateEssayByStudentPayload = objectType({
  name: 'UpdateEssayByStudentPayload',
  definition(t) {
    t.field('essay', { type: Essay })
  },
})

export const UpdateEssayByStudent = mutationField('updateEssayByStudent', {
  type: UpdateEssayByStudentPayload,
  description:
    'To change an individual students topic question information or dueDate',
  args: { input: arg({ type: UpdateEssayByStudentInput, required: true }) },
  async resolve(
    _,
    { input: { studentId, section, topic, dueDate } },
    { assignmentData }
  ) {
    const essayValidation: NexusGenRootTypes['Essay'][] = await assignmentData.findOne(
      {
        'hasOwner._id': new ObjectId(studentId),
        'readings.readingSections': section,
      }
    )
    if (essayValidation) {
      await assignmentData.updateOne(
        {
          'hasOwner._id': new ObjectId(studentId),
          'readings.readingSections': section,
        },
        {
          $set: {
            topic,
            dueDate,
          },
        }
      )
      const essay = await assignmentData.findOne({
        'hasOwner._id': new ObjectId(studentId),
        'readings.readingSections': section,
      })
      return { essay }
    } else throw new Error('Essay not found')
  },
})