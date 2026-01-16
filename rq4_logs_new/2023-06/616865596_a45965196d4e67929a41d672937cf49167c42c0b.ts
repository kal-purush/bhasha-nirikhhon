import { API } from './api'

export const answerApi = {
  getAnswer: async (date: string) => await API.get('/api/answer', { params: { date } }),

  getAnswerRecordCount: async (date: string) => await API.get('/api/answer/record', { params: { date } }),

  getMyAnswerList: async (month: string, year: string) =>
    await API.get('/api/answer/list', { params: { month, year } }),

  postAnswer: async (date: string, content: string, emotion: number) =>
    await API.post(
      '/api/answer',
      { content, emotion, date },
      {
        headers: { 'Content-Type': 'application/json' }
      }
    ),

  deleteAnswer: async (date: string) => await API.post('/api/answer/delete', { params: { date } }),

  updateAnswer: async (date: string, content: string) => await API.patch('/api/answer/update', { date, content })
}