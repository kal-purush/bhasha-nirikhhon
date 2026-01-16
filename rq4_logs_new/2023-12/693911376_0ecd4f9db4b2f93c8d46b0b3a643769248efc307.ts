import { AxiosResponse } from 'axios'
import api from '../api'
import { TopicData } from '../utils/interfaces'

export const getAllTopics = async (): Promise<AxiosResponse> => {
  const response = await api.get('topics')

  return response
}

export const getOneTopic = async (id: number): Promise<AxiosResponse> => {
  const response = await api.get(`topics/${id}`)

  return response
}

export const createTopic = async (data: TopicData): Promise<AxiosResponse> => {
  const response = await api.post(`topics`, data)

  return response
}