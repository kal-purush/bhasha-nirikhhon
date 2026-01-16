const axios = require('axios')
const BASE_URL = ''

// POST enrollment/student=[student_id]
//                 &course=[course_id]
//                 &term=(fall|spring)
//                 &year=[year_int]
export async function enrollStudent (studentId, courseId, term, year) {
  const response = await axios.post(`${BASE_URL}/enrollment/student=${studentId}&course=${courseId}&term=${term}&year=${year}`)
  return response
}

// GET student/<student_id>
export async function getStudent (studentId) {
  const response = await axios.get(`${BASE_URL}/student=${studentId}`)
  return response
}