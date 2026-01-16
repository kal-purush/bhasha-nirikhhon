import axios from "axios"

const testData = {
  id: "666",
  name: "Маргарита",
  patronymic: "Ивановна",
  surname: "Веселова",
  birthdate: "2000-01-01",
  citizenship: "Россия",
  location: "Москва",
  phone: "88005553535",
  email: "gZa8a@example.com",
}

interface QueryResponse {
  id: string
  avatar: string | null
  name: string
  patronymic: string
  surname: string
  birthdate: string
  citizenship: string
  location: string
  phone: string
  email: string
}

export const fetchProfileInfo = async () => {
  return axios
    .get("https://api.publicapis.org/entries")
    .then((res) => testData as QueryResponse)
}