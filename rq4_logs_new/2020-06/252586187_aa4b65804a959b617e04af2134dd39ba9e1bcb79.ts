import { objectType, inputObjectType, arg, mutationField } from '@nexus/schema'
import { Student } from './students'
import { ObjectId } from 'mongodb'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'

export const RemoveStudentsFromCourseInput = inputObjectType({
  name: 'RemoveStudentsFromCourseInput',
  definition(t) {
    t.list.id('studentIds', { required: true })
    t.id('courseId', { required: true })
  },
})

export const RemoveStudentsFromCoursePayload = objectType({
  name: 'RemoveStudentsFromCoursePayload',
  definition(t) {
    t.list.field('students', { type: Student })
  },
})

export const RemoveStudentsFromCourse = mutationField(
  'removeStudentsFromCourse',
  {
    type: RemoveStudentsFromCoursePayload,
    args: {
      input: arg({ type: RemoveStudentsFromCourseInput, required: true }),
    },
    async resolve(
      _,
      { input: { studentIds, courseId } },
      { userData, courseData }
    ) {
      const course = await courseData.findOne({ _id: new ObjectId(courseId) })
      if (!course) throw new Error('Course does not exist!')

      const studentsToRemove: NexusGenRootTypes['Student'][] = []
      const updatedStudents: NexusGenRootTypes['Student'][] = []

      const studentNotInCourse: NexusGenRootTypes['Student'][] = []

      for (const _id of studentIds) {
        const student: NexusGenRootTypes['Student'] = await userData.findOne({
          _id: new ObjectId(_id),
        })

        if (
          student.inCourses.some(
            (studentCourses) =>
              studentCourses._id?.toString() === course._id.toString()
          )
        ) {
          await userData.updateOne(
            { _id: new ObjectId(student._id!) },
            { $pull: { inCourses: course } }
          )
          const updatedStudent = await userData.findOne({
            _id: new ObjectId(student._id!),
          })
          updatedStudents.push(updatedStudent)
        } else studentNotInCourse.push(student)
      }

      return { students: updatedStudents }
    },
  }
)