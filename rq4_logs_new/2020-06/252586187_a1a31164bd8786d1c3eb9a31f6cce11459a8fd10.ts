import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { Essay } from '.'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'

export const UnAssignEssaysInput = inputObjectType({
  name: 'UnAssignEssaysInput',
  definition(t) {
    t.list.id('studentIds', { required: true })
    t.string('section', { required: true })
  },
})

export const UnAssignEssaysPayload = objectType({
  name: 'UnAssignEssaysPayload',
  definition(t) {
    t.list.field('essays', { type: Essay })
  },
})

export const UnAssignEssays = mutationField('unAssignEssays', {
  type: UnAssignEssaysPayload,
  args: { input: arg({ type: UnAssignEssaysInput, required: true }) },
  async resolve(_, { input: { studentIds, section } }, { assignmentData }) {
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
              assigned: false,
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