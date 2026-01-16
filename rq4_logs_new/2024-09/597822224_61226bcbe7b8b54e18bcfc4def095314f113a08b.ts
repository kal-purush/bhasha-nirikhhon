import axios, { AxiosRequestConfig } from 'axios'
import { Draggable, BackgroundColor } from '../interfaces'
import { ELanguages } from '../../../interfaces'

const url = import.meta.env.VITE_BASE_URI ?? 'https://bg.jenniina.fi'
const baseUrl = `${url}/api/blobs`

// router.get('/api/blobs/:user/:language', getAllBlobsByUser)
// router.get('/api/blobs/:user/:versionName/:language', getBlobsVersionByUser)
// router.post('/api/blobs/:user/:versionName/:language', saveBlobsByUser)
// router.delete('/api/blobs/:user/:versionName/:language',

const getAllBlobsByUser = async (user: string, d: number, language: ELanguages) => {
  const response = await axios.get(`${baseUrl}/${user}/${d}`, { params: { language } })
  return response.data
}

const getBlobsVersionByUser = async (
  user: string,
  d: number,
  versionName: string,
  language: ELanguages
) => {
  const response = await axios.get(`${baseUrl}/${user}/${d}/${versionName}/${language}`)
  return response.data
}

const saveBlobsByUser = async (
  user: string,
  d: number,
  draggables: Draggable[],
  versionName: string,
  backgroundColor: BackgroundColor[],
  language: ELanguages
) => {
  const response = await axios.post(
    `${baseUrl}/${user}/${d}/${versionName}/${language}`,
    {
      draggables,
      backgroundColor,
    }
  )
  return response.data
}

const editBlobsByUser = async (
  user: string,
  d: number,
  draggables: Draggable[],
  versionName: string,
  backgroundColor: BackgroundColor[],
  language: ELanguages,
  newVersionName: string
) => {
  const response = await axios.put(`${baseUrl}/${user}/${d}/${versionName}/${language}`, {
    d,
    versionName,
    draggables,
    backgroundColor,
    newVersionName,
  })
  return response.data
}

const deleteBlobsVersionByUser = async (
  user: string,
  d: number,
  versionName: string,
  language: ELanguages
) => {
  const response = await axios.delete(
    `${baseUrl}/${user}/${d}/${versionName}/${language}`
  )
  return response.data
}

export default {
  getAllBlobsByUser,
  getBlobsVersionByUser,
  saveBlobsByUser,
  editBlobsByUser,
  deleteBlobsVersionByUser,
}