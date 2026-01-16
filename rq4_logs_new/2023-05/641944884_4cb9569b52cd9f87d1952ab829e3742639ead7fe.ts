import axios from "axios"

const testData = [
  {
    id: "1902",
    date: "20 иолюя 2023",
    title: "UX/UI дизайнер",
    company: "Карьерный центр Правительства Москвы",
    status: "active",
    mentorName: "Юлиана Митрофанова",
    mentorRole: "Наставник",
    mentorAvatar: null,
    responsesCount: 19,
    responsesCountNew: 5,
  },
]

interface VacancyData {
  id: string
  date: string
  title: string
  company: string
  status: string
  mentorName: string
  mentorRole: string
  mentorAvatar: string | null
  responsesCount: number
  responsesCountNew: number
}

interface QueryParams {
  page: number
  search: string
  statuses: string[]
  mentors: string[]
}

interface QueryResponse {
  items: VacancyData[]
  page: number
  totalPages: number
}

export const fetchVacancyList = async ({ page }: QueryParams) => {
  return axios.get("").then(
    (res) =>
      ({
        items: testData,
        page: 5,
        totalPages: 10,
      } as QueryResponse)
  )
}