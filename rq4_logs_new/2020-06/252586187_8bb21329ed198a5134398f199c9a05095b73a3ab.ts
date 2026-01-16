import { objectType, inputObjectType, arg, queryField } from '@nexus/schema'
import { Essay } from '.'
import { ObjectId } from 'mongodb'

export const FindEssaysToCompleteByStudentIdInput = inputObjectType({
  name: 'FindEssaysToCompleteByStudentIdInput',
  definition(t) {
    t.id('studentId', { required: true })
  },
})

export const FindEssaysToCompleteByStudentIdPayload = objectType({
  name: 'FindEssaysToCompleteByStudentIdPayload',
  definition(t) {
    t.list.field('essays', { type: Essay })
  },
})

export const FindEssaysToCompleteByStudentId = queryField(
  'findEssaysToCompleteByStudentId',
  {
    type: FindEssaysToCompleteByStudentIdPayload,
    args: {
      input: arg({
        type: FindEssaysToCompleteByStudentIdInput,
        required: true,
      }),
    },
    async resolve(_, { input: { studentId } }, { assignmentData }) {
      const essays = await assignmentData
        .find({ 'hasOwner._id': new ObjectId(studentId), assigned: true })
        .toArray()
      return { essays }
    },
  }
)