import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { StudentAbsence } from './studentAbsence'
import { MarkingPeriodEnum } from '../general'
import { ObjectId } from 'mongodb'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'

export const CreateAbsenceInput = inputObjectType({
  name: 'CreateAbsenceInput',
  definition(t) {
    t.string('studentId', { required: true })
    t.date('dayAbsent', { required: true })
    t.field('markingPeriod', { type: MarkingPeriodEnum, required: true })
  },
})

export const CreateAbsencePayload = objectType({
  name: 'CreateAbsencePayload',
  definition(t) {
    t.field('studentAbsence', { type: StudentAbsence })
  },
})

export const CreateAbsence = mutationField('createAbsence', {
  type: CreateAbsencePayload,
  args: { input: arg({ type: CreateAbsenceInput, required: true }) },
  async resolve(
    _,
    { input: { studentId, dayAbsent, markingPeriod } },
    { studentData, userData }
  ) {
    const student = await userData.findOne({ _id: new ObjectId(studentId) })

    const studentAbsence: NexusGenRootTypes['StudentAbsence'] = {
      student,
      dayAbsent,
      markingPeriod,
    }

    const insertedId = await studentData.insertOne(studentAbsence)
    studentAbsence._id = insertedId

    return { studentAbsence }
  },
})