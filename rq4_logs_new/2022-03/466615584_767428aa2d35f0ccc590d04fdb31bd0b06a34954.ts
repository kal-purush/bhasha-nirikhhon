import { IContacts } from "../utils/interfaces";

//USER TEMPLATE
// {
//   firstName: "",
//   lastName: "",
//   email: "",
//   phoneNumber: "",
//   favorite: false,
//   id: 101,
// }

const contacts: IContacts = [
  {
    firstName: "Kevin",
    lastName: "Hartmann",
    email: "kevinhartmann23@gmail.com",
    phoneNumber: "12038898429",
    favorite: true,
    id: 1000000000001,
  },
  {
    firstName: "Bill",
    lastName: "Murray",
    email: "bill@ghostbusters.org",
    phoneNumber: "18005552368",
    favorite: true,
    id: 1000000000002,
  },
  {
    phoneNumber: "(901) 583-8236",
    email: "dictum.placerat@aol.net",
    firstName: "Bernard",
    favorite: false,
    id: 1661146611,
    lastName: "Cooke"
  },
  {
    phoneNumber: "(456) 944-3953",
    email: "felis.ullamcorper@outlook.net",
    firstName: "Xandra",
    favorite: false,
    id: 1645578518,
    lastName: "Pena"
  },
  {
    phoneNumber: "1-734-856-7028",
    email: "fusce.diam@aol.net",
    firstName: "Sydney",
    favorite: false,
    id: 1665164183,
    lastName: "Barry"
  },
  {
    phoneNumber: "(786) 558-5540",
    email: "ultrices@protonmail.org",
    firstName: "Francesca",
    favorite: false,
    id: 1671728709,
    lastName: "Blankenship"
  },
  {
    phoneNumber: "(265) 326-2785",
    email: "erat.etiam@google.net",
    firstName: "Robin",
    favorite: false,
    id: 1621051007,
    lastName: "Nunez"
  }
] 

module.exports = contacts