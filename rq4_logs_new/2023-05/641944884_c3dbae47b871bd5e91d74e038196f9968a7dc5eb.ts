import axios from "axios"

const testData = {
  id: "1902",
  title: "UX/UI дизайнер",
  description:
    "Прежде всего, постоянное информационно-пропагандистское обеспечение нашей деятельности, в своём классическом представлении, допускает внедрение приоретизации разума над эмоциями. Таким образом, сплочённость команды профессионалов, а также свежий взгляд на привычные вещи — безусловно открывает новые горизонты для укрепления.",
  status: "active",
  rejectionReason:
    "У вас уже создана аналогичная вакансия на выбранные даты. Укажите другие сроки стажировки или измените данные вакансии и подайте заявку повторно.",
  company: {
    name: "АНО «Проектный офис по развитию туризма и гостеприимства Москвы», пресс-центр",
    address: "Калужская, ул. Большая Дмитровка, 7/5",
    reviews: {
      count: 24,
      averageRate: 4.7,
    },
  },
  mentor: {
    id: "1",
    name: "Юлиана Митрофанова",
    avatar: null,
    reviews: {
      count: 24,
      averageRate: 4.7,
    },
  },
  internship: {
    startDate: "2022-08-01",
    endDate: "2022-09-30",
  },
  testTask: {
    description:
      "Тут краткое описание текстового задания и может быть добавлена ссылка, например, https://lk.leaders2023.innoagency.ru/todo и файл.",
    fileName: "Document_name... .pdf",
    fileSize: "10 MB",
  },
  responses: {
    count: 19,
    countNew: 5,
    items: [
      {
        id: "101",
        status: "new",
        name: "Марина Высокова",
        age: "22",
        score: 20,
        address: "г. Москва",
        education: "МГУ им. Ломоносова, выпуск 2023 г.",
        isNew: true,
        reviews: {
          count: 24,
          averageRate: 4.7,
        },
      },
    ],
  },
}

interface QueryParams {
  id: string
}

export interface ResponseData {
  id: string
  status: string
  name: string
  age: string
  score: number
  address: string
  education: string
  avatar: string | null
  isNew: boolean
  reviews: {
    count: number
    averageRate: number
  }
}

interface QueryResponse {
  id: string
  title: string
  description: string
  status: string
  rejectionReason: string | null
  company: {
    name: string
    address: string
    reviews: {
      count: number
      averageRate: number
    }
  }
  mentor: {
    id: string
    name: string
    avatar: string | null
    reviews: {
      count: number
      averageRate: number
    }
  }
  internship: {
    startDate: string
    endDate: string
  }
  testTask: {
    description: string
    fileName: string
    fileSize: string
  }
  responses: {
    count: number
    countNew: number
    items: ResponseData[]
  }
}

export const fetchVacancyInfo = async ({ id }: QueryParams) => {
  return axios
    .get("https://api.publicapis.org/entries")
    .then((res) => testData as QueryResponse)
}