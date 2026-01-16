import { objectType, inputObjectType, arg, queryField } from '@nexus/schema'
import { Course } from '.'
import { NexusGenRootTypes } from 'teachers-aid-server/src/teachers-aid-typegen'
import { ObjectId } from 'mongodb'

export const FindCoursesByIdInput = inputObjectType({
  name: 'FindCoursesByIdInput',
  definition(t) {
    t.list.id('_ids', { required: true })
  },
})

export const FindCoursesByIdPayload = objectType({
  name: 'FindCoursesByIdPayload',
  definition(t) {
    t.list.field('courses', { type: Course })
  },
})

export const FindCoursesById = queryField('findCoursesById', {
  type: FindCoursesByIdPayload,
  args: { input: arg({ type: FindCoursesByIdInput, required: true }) },
  async resolve(_, { input: { _ids } }, { courseData }) {
    const courses: NexusGenRootTypes['Course'][] = []
    for (const _id of _ids) {
      const course = await courseData.findOne({ _id: new ObjectId(_id) })
      courses.push(course)
    }
    return { courses }
  },
})