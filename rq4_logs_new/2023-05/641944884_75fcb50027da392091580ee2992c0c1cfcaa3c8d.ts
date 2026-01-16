import axios from "axios"

const testData = {
  id: "101",
  user: {
    name: "Марина Высокова",
    age: 22,
    address: "г. Москва",
    phone: "+7 (910) 234-56-78",
    email: "marina@gmail.com",
    reviews: {
      count: 24,
      averageRate: 4.7,
    },
  },

  score: 20,
  schedule: "20 часов в неделю",
  experience:
    "ООО «Рога и копыта» с мая 2022 по май 2023. Ведение социальных сетей, придумывание рекламных креативов АНО «Объединение умов» с января по май 2022. Создание контента для пиара мероприятий",
  projectActivity:
    "В своём стремлении улучшить пользовательский опыт мы упускаем, что базовые сценарии поведения пользователей объективно рассмотрены соответствующими инстанциями. Прежде всего, социально-экономическое развитие в значительной степени обусловливает обусловливает важность переосмысления.",
  about:
    "Противоположная точка зрения подразумевает, что сделанные на базе интернет-аналитики выводы рассмотрены исключительно в разрезе маркетинговых и финансовых предпосылок. Банальные, но неопровержимые выводы, а также независимые государства, вне зависимости от их уровня.",
  education: {
    name: "Московский государственный университет им. М.Ломоносова",
    specialty: "Юридический факультет",
    graduationYear: "2024",
  },
  status: "new",
  rejectionReason:
    "Спасибо за ваш отклик. К сожалению, пока что мы не готовы взять вас на стажировку.",
  testTask: {
    fileName: "Document_name... .pdf",
    fileSize: "2 MB",
  },
  coveringLetter:
    "Добрый день. Я бы очень хотела работать у вас. У меня есть для этого необходимый опыт и я знаю, как сделать ваши пресс релизы более яркими и интересными.",
}

interface QueryParams {
  id: string
}

interface QueryResponse {
  id: string
  user: {
    name: string
    avatar: string | null
    age: number
    address: string
    phone: string
    email: string
    reviews: {
      count: number
      averageRate: number
    }
  }

  score: number

  schedule: string

  experience: string
  projectActivity: string
  about: string
  education: {
    name: string
    specialty: string
    graduationYear: string
  }
  status: string
  rejectionReason: string
  testTask: {
    fileName: string
    fileSize: string
  }
  coveringLetter: string
}

export const fetchInternInfo = async ({ id }: QueryParams) => {
  return axios
    .get("https://api.publicapis.org/entries")
    .then((res) => testData as QueryResponse)
}