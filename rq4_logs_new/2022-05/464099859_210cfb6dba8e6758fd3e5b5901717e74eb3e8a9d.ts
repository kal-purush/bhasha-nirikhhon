import { Course } from 'types/course'

export const getCourses = async (
  courseID: number,
  progressionID: number,
  progressionCompleted: number
): Promise<Course> => {
  try {
    const dataCourses = await fetch(
      `http://localhost:4000/course?id=${courseID}`
    )
    const json = await dataCourses.json()
    json.completed = progressionCompleted
    json.progressionID = progressionID
    return json
  } catch (e) {
    throw new Error(`Fail to fetch courses: ${e}`)
  }
}