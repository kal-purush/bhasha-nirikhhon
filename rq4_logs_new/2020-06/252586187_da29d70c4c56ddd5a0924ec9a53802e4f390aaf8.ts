import { objectType, inputObjectType, arg, queryField } from '@nexus/schema'
import { Essay } from '.'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'

export const FindEssaysToGradeByIdInput = inputObjectType({
  name: 'FindEssaysToGradeByIdInput',
  definition(t) {
    t.id('teacherId', { required: true })
  },
})

export const FindEssaysToGradeByIdPayload = objectType({
  name: 'FindEssaysToGradeByIdPayload',
  definition(t) {
    t.list.field('essays', { type: Essay })
  },
})

export const FindEssaysToGradeById = queryField('findEssaysToGradeById', {
  type: FindEssaysToGradeByIdPayload,
  args: { input: arg({ type: FindEssaysToGradeByIdInput, required: true }) },
  async resolve(_, { input: { teacherId } }, { assignmentData }) {
    const essays: NexusGenRootTypes['Essay'][] = await assignmentData
      .find({
        'hasAssigner._id': new ObjectId(teacherId),
        finalDraft: { $exists: true },
      })
      .toArray()

    return { essays }
  },
})