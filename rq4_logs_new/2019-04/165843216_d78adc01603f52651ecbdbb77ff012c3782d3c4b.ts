import { ThunkAction } from 'redux-thunk'
import { AnyAction } from 'redux'
import courseService from '../../services/courseService'
import { TeachingInstance, UserCourse } from '../../types/jsontypes'

export const FETCH_TEACHER_COURSES = 'FETCH_TEACHER_COURSES'
export const SET_TEACHER_COURSES = 'SET_TEACHER_COURSES'
export const FETCH_OWN_COURSES = 'FETCH_OWN_COURSES'
export const SET_OWN_COURSES = 'SET_OWN_COURSES'
export const JOIN_TEACHING_INSTANCE = 'JOIN_TEACHING_INSTANCE'
export const ADD_OWN_COURSE = 'ADD_OWN_COURSE'
export const ADD_TECHER_COURSE = 'ADD_TEACHER_COURSE'

export const setTeacherCourses = (courses: any[]) => ({
  type: SET_TEACHER_COURSES,
  data: courses
})

export const fetchTeacherCourses = (): ThunkAction<Promise<any[]>, {}, {}, AnyAction> => {
  return async dispatch =>
    courseService.teacherCourses().then((courses: any[]) => {
      dispatch(setTeacherCourses(courses))
      return courses
    })
}

export const setOwnCourses = (courses: any[]) => ({
  type: SET_OWN_COURSES,
  data: courses
})

export const addOwnCourse = (course: any) => ({
  type: ADD_OWN_COURSE,
  data: course
})

export const addTeacherCourse = (teacherCourse: UserCourse) => ({
  type: ADD_TECHER_COURSE,
  data: teacherCourse
})

export const joinTeachingInstance = (coursekey: string): ThunkAction<Promise<any[]>, {}, {}, AnyAction> => {
  return async dispatch =>
    courseService.joinTeachingInstanceService(coursekey).then((teachingInstance: any) => {
      dispatch(addOwnCourse(teachingInstance))
      return teachingInstance
    })
}

export const fetchOwnCourses = (): ThunkAction<Promise<any[]>, {}, {}, AnyAction> => {
  return async dispatch =>
    courseService.ownCourses().then((courses: any[]) => {
      dispatch(setOwnCourses(courses))
      return courses
    })
}

export const createTeacherCourse = (teachingInstance: TeachingInstance): ThunkAction<Promise<any[]>, {}, {}, AnyAction> => {
  return async dispatch =>
    courseService.createTeachingInstance(teachingInstance)
      .then(createdInstance => {
        dispatch(addTeacherCourse(createdInstance))
        return createdInstance
      })
}