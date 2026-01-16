import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { MarkingPeriodEnum } from '../general'
import { UnexcusedLateness } from '.'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'
import { ExcusedLateness } from './excusedLateness'

export const CreateExcusedLatenessInput = inputObjectType({
  name: 'CreateExcusedLatenessInput',
  definition(t) {
    t.id('studentId', { required: true })
    t.date('dayLateExcused', { required: true })
    t.field('markingPeriod', { type: MarkingPeriodEnum, required: true })
  },
})

export const CreateExcusedLatenessPayload = objectType({
  name: 'CreateExcusedLatenessPayload',
  definition(t) {
    t.field('excusedLateness', { type: ExcusedLateness })
  },
})

export const CreateExcusedLateness = mutationField('createExcusedLateness', {
  type: CreateExcusedLatenessPayload,
  args: {
    input: arg({ type: CreateExcusedLatenessInput, required: true }),
  },
  async resolve(
    _,
    { input: { studentId, dayLateExcused, markingPeriod } },
    { userData, studentData }
  ) {
    const student: NexusGenRootTypes['Student'] = await userData.findOne({
      _id: new ObjectId(studentId),
    })

    const studentLatenesses: NexusGenRootTypes['ExcusedLateness'][] = await studentData
      .find({
        'student._id': new ObjectId(student._id!),
        dayLateExcused: { $exists: true },
      })
      .toArray()

    const latenessSearch = studentLatenesses.some(
      (lateness) => lateness.dayLateExcused === dayLateExcused
    )
    if (!latenessSearch) {
      const excusedLateness: NexusGenRootTypes['ExcusedLateness'] = {
        student,
        dayLateExcused,
        markingPeriod,
      }
      const insertedId = await studentData.insertOne(excusedLateness)
      excusedLateness._id = insertedId

      return { excusedLateness }
    } else
      throw new Error(
        student.userName + ' already has absence assigned for ' + dayLateExcused
      )
  },
})