import { NewITest } from '../store/tests-store'

export const createTest = (test: NewITest) => {
  const requestURL = `http://localhost:5000/tests/create`
  const requestOptions = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      data: test,
    }),
  }

  fetch(requestURL, requestOptions)
    .then((response) => response.json())
    .then((data) => {
      // setQuestions(data.payload.questions)
      // getTests()
    })
}

export const getTests = () => {
  const requestURL = `http://localhost:5000/tests/`
  const requestOptions = {
    method: 'GET',
    headers: { 'Content-Type': 'application/json' },
  }

  return fetch(requestURL, requestOptions)
    .then((response) => response.json())
    .then((data) => {
      return data
    })
}

export const deleteTestById = (testId: string) => {
  const requestURL = `http://localhost:5000/tests/${testId}`
  const requestOptions = {
    method: 'DELETE',
    headers: { 'Content-Type': 'application/json' },
  }

  return fetch(requestURL, requestOptions)
    .then((response) => response.json())
    .then((data) => {
      console.log(data.message)
    })
}