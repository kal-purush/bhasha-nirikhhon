import { inputObjectType, objectType, mutationField, arg } from '@nexus/schema'
import { Essay } from '.'
import { MarkingPeriodEnum } from '../../general'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'
import { QuestionTypeEnum } from '../..'

export const AssignEssaysInput = inputObjectType({
  name: 'AssignEssaysInput',
  definition(t) {
    t.list.id('studentIds', { required: true })
    t.string('section', { required: true })
    t.date('assignedDate', { required: true })
    t.date('dueDate', { required: true })
  },
})

export const AssignEssaysPayload = objectType({
  name: 'AssignEssaysPayload',
  definition(t) {
    t.list.field('essays', { type: Essay })
  },
})

export const AssignEssays = mutationField('assignEssays', {
  type: AssignEssaysPayload,
  args: { input: arg({ type: AssignEssaysInput, required: true }) },
  async resolve(
    _,
    { input: { studentIds, section, assignedDate, dueDate } },
    { assignmentData }
  ) {
    const essays: NexusGenRootTypes['Essay'][] = []

    for (const _id of studentIds) {
      const essayValidation: NexusGenRootTypes['Essay'][] = await assignmentData.findOne(
        {
          'hasOwner._id': new ObjectId(_id),
          'readings.readingSections': section,
        }
      )
      if (essayValidation) {
        await assignmentData.updateOne(
          {
            'hasOwner._id': new ObjectId(_id),
            'readings.readingSections': section,
          },
          {
            $set: {
              dueDate,
              assignedDate, //have client put in currentDate
              assigned: true,
            },
          }
        )
      }
      const essay = await assignmentData.findOne({
        'hasOwner._id': new ObjectId(_id),
        'readings.readingSections': section,
      })
      essays.push(essay)
    }
    return { essays }
  },
})