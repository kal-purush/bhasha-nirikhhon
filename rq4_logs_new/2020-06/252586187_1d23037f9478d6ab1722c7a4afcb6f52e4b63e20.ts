import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { ResponsibilityPoints } from '.'
import { MarkingPeriodEnum } from '../general'
import { ObjectId } from 'mongodb'

export const UpdateResponsibilityPointsInput = inputObjectType({
  name: 'UpdateResponsibilityPointsInput',
  definition(t) {
    t.id('studentId', { required: true })
    t.field('markingPeriod', { type: MarkingPeriodEnum })
    t.int('points', { required: true })
  },
})

export const UpdateResponsibilityPointsPayload = objectType({
  name: 'UpdateResponsibilityPointsPayload',
  definition(t) {
    t.field('responsibilityPoints', { type: ResponsibilityPoints })
  },
})

export const UpdateResponsibilityPoints = mutationField(
  'updateResponsibilityPoints',
  {
    type: UpdateResponsibilityPointsPayload,
    args: {
      input: arg({ type: UpdateResponsibilityPointsInput, required: true }),
    },
    async resolve(
      _,
      { input: { studentId, points, markingPeriod } },
      { studentData }
    ) {
      await studentData.updateOne(
        {
          'student._id': new ObjectId(studentId),
          responsibilityPoints: { $exists: true },
          markingPeriod,
        },
        {
          $inc: { responsibilityPoints: points },
        }
      )
      const responsibilityPoints = await studentData.findOne({
        'student._id': new ObjectId(studentId),
        responsibilityPoints: { $exists: true },
        markingPeriod,
      })
      return { responsibilityPoints }
    },
  }
)