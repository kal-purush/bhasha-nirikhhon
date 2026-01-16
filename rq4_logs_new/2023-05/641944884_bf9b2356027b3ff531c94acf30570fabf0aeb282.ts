import axios from "axios"

const testData = [
  {
    id: "017",
    name: "Юлиана Митрофанова",
    avatar: null,
    score: 5,
    status: "new",
    age: 22,
    address: "г. Москва",
    education: "МГУ им. Ломоносова, выпуск 2023 г.",
    reviews: {
      count: 24,
      averageRate: 4.7,
    },
  },
  {
    id: "008",
    name: "Марина Высокова",
    avatar: null,
    score: 20,
    status: "new",
    age: 22,
    address: "г. Москва",
    education: "МГУ им. Ломоносова, выпуск 2023 г.",
    reviews: {
      count: 10,
      averageRate: 4.5,
    },
  },
]

export interface InternData {
  id: string
  name: string
  avatar: string | null
  status: string
  score: number
  age: number
  address: string
  education: string
  reviews: {
    count: number
    averageRate: number
  }
}

interface QueryParams {
  page: number
  search: string
}

interface QueryResponse {
  items: InternData[]
  page: number
  totalPages: number
}

export const fetchInternList = async ({ page }: QueryParams) => {
  return axios.get("https://api.publicapis.org/entries").then(
    (res) =>
      ({
        items: testData,
        page: 5,
        totalPages: 10,
      } as QueryResponse)
  )
}