import { LessonsLight } from './lesson'

export interface CourseSummaryProps {
  courseID: number
  courseName: string
  duration: number
  difficulty: number
  completed: number
  numberLesson: number
  numberQuiz: number
  tags: string[]
  handleCourse: (courseID: number) => void
  isNotEnrolled: boolean
}

interface Course {
  courseID: number
  courseName: string
  duration: number
  difficulty: number
  completed: number
  numberLesson: number
  numberQuiz: number
  tags: string[]
}

export interface CourseContentProps {
  lessons: LessonsLight
  tags: string[]
  course: Course
}