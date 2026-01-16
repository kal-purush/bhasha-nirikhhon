import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { Student } from '.'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'
import { MarkingPeriodEnum } from '../general'
import { StudentInformation } from './studentInformation'
import { OverallWritingMetric } from './progress-metrics'

export const InitializeStudentsInput = inputObjectType({
  name: 'InitializeStudentsInput',
  definition(t) {
    t.list.id('studentIds', { required: true })
    t.field('markingPeriod', { type: MarkingPeriodEnum, required: true })
  },
})

export const InitializeStudentsPayload = objectType({
  name: 'InitializeStudentsPayload',
  definition(t) {
    t.list.field('students', { type: Student })
    // t.list.field('studentinformationList', { type: StudentInformation })
  },
})

export const InitializeStudents = mutationField('initializeStudents', {
  type: InitializeStudentsPayload,
  args: { input: arg({ type: InitializeStudentsInput, required: true }) },
  async resolve(
    _,
    { input: { studentIds, markingPeriod } },
    { userData, studentData }
  ) {
    let studentList: NexusGenRootTypes['ResponsibilityPoints'][] = []

    let studentWithResponsibilityPoints = []

    for (const _id of studentIds) {
      const student = await userData.findOne({ _id: new ObjectId(_id) })
      const responsibilityPointsCheck = await studentData.findOne({
        'student._id': new ObjectId(_id),
        responsibilityPoints: { $exists: true },
        markingPeriod: markingPeriod,
      })

      if (!responsibilityPointsCheck) {
        const responsibilityPoints: NexusGenRootTypes['ResponsibilityPoints'] = {
          markingPeriod,
          responsibilityPoints: 100,
          student,
        }
        const insertedId = await studentData.insertOne(responsibilityPoints)
        responsibilityPoints._id = insertedId
        studentList.push(responsibilityPoints)
      } else studentWithResponsibilityPoints.push(student)
    }

    const studentsWithContactInfo: NexusGenRootTypes['StudentInformation'][] = []

    for (const _id of studentIds) {
      const student = await userData.findOne({ _id: new ObjectId(_id) })
      const contactInfoCheck = await studentData.findOne({
        'student._id': new ObjectId(_id),
        contactInfo: { $exists: true },
      })
      if (!contactInfoCheck) {
        const studentInformation: NexusGenRootTypes['StudentInformation'] = {
          student,
          contactInfo: [
            {
              guardianFirstName: '',
              guardianLastName: '',
              guardianPhone: '',
              guardianEmail: '',
            },
          ],
        }
        const insertedId = await studentData.insertOne(studentInformation)
        studentInformation._id = insertedId
      }
      studentsWithContactInfo.push(student)
    }

    const studentsWithPreExistingWritingMetrics = []

    for (const _id of studentIds) {
      const student = await userData.findOne({ _id: new ObjectId(_id) })
      const studentWritingMetric = await studentData.findOne({
        'student._id': new ObjectId(_id),
        howCauseEffectMetrics: { $exists: true },
      })
      if (!studentWritingMetric) {
        const writingMetric: NexusGenRootTypes['WritingMetrics'] = {
          student,
          overallWritingMetric: {
            overallWritingLevel: 'DEVELOPING',
            levelPoints: 0,
          },
          howCauseEffectMetrics: {
            howCauseEffectLevel: 'DEVELOPING',
            levelPoints: 0,
          },
          howProblemSolutionMetrics: {
            howProblemSolutionLevel: 'DEVELOPING',
            levelPoints: 0,
          },
          whyCauseEffectMetrics: {
            whyCauseEffectLevel: 'DEVELOPING',
            levelPoints: 0,
          },
        }
        const insertedId = await studentData.insertOne(writingMetric)
        writingMetric._id = insertedId
      } else studentsWithPreExistingWritingMetrics.push(student)
      // if(studentWritingMetric) {} studentsWithPreExistingWritingMetrics.push(student)
    }

    const students: NexusGenRootTypes['Student'][] = []
    for (const _id of studentIds) {
      const student = await userData.findOne({ _id: new ObjectId(_id) })
      students.push(student)
    }
    return { students }
  },
})