import { AxiosResponse } from 'axios'
import api from '../api'

export const makeUpvoteComment = async (
  commentId: number,
  topicId: number,
): Promise<AxiosResponse> => {
  const response = await api.post(
    `topics/${topicId}/comments/${commentId}/upvote`,
  )

  return response
}

export const makeDownvoteComment = async (
  commentId: number,
  topicId: number,
): Promise<AxiosResponse> => {
  const response = await api.post(
    `topics/${topicId}/comments/${commentId}/downvote`,
  )

  return response
}