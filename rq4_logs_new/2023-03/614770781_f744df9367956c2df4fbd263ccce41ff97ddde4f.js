import listActions from '../stores/actionType';
const baseUrl = 'http://localhost:3000'

// User
export const postLogin = ({ username, password }) => {
  return async (dispatch, getState) => {
    try {
      const response = await fetch( `${baseUrl}/users/login`,{
        method:'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({username, password})
      })
      const responsJSON = await response.json()
      if (!response.ok){
        throw responsJSON
      } else {
        await localStorage.setItem('access_token', responsJSON.data.access_token)
        return {data: responsJSON, error:{}}
      }
    } catch (response) {
      return {data: null, error:response}
    }
  }
}

export const postRegister = ( username, password ) => {
  return async (dispatch, getState) => {
    try {
      const response = await fetch( `${baseUrl}/users/register`,{
        method:'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({username, password})
      })
      const responsJSON = await response.json()
      if (!response.ok){
        throw responsJSON
      } else {
        return {data: responsJSON, error:null}
      }
    } catch (response) {
      return {data: null, error:response}
    }
  }
}

// Jobs
export const getJobs = (activePage, searchQuery) => {
  return async (dispatch, getState) => {
    try {
      const {description, type, location} = searchQuery
      const query = `?page=${activePage?activePage:1}${description?`&description=${description}`:''}${type?`&full_time=${type}`:''}${location?`&location=${location}`:''}`
      const response = await fetch( `${baseUrl}/jobs${query}`,{
        method:'GET',
        headers: {
          'Content-Type': 'application/json',
          'access_token': localStorage.getItem('access_token')
        }
      })
      const responsJSON = await response.json()
      const {data, page, totalPage} = responsJSON
      const payload = {jobs:data, page, totalPage}
      dispatch({type:listActions.getJobs, payload})
    } catch (error) {
      console.log(error)
    }
  }
}

export const getJobById = (id) => {
  return async (dispatch, getState) => {
    try {
      const response = await fetch( `${baseUrl}/jobs/${id}`,{
        method:'GET',
        headers: {
          'Content-Type': 'application/json',
          'access_token': localStorage.getItem('access_token')
        }
      })
      const responsJSON = await response.json()
      const { data } = responsJSON
      const payload = {jobById:data}
      dispatch({type:listActions.getJobById, payload})
    } catch (error) {
      console.log(error)
    }
  }
}