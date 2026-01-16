import Account from "../model/Account";

export const shopAccount: Account = {
  id: "999",
  createdDate: new Date(),
  email: "Companyinnovationcenter@gmail.com",
  username: "Company",
  phone: "+84 12 345 6789",
  imageUrl:
    "https://www.mintformations.co.uk/blog/wp-content/uploads/2020/05/shutterstock_583717939.jpg",
  isDisable: false,
  isLocked: false,
  role: "ADMIN",
  addresses: [
    {
      id: 1,
      street: "7th Floor, Etown 5, Cong Hoa Str.",
      district: "Tan Binh District",
      city: "Ho Chi Minh City",
      country: "Vietnam",
    },
  ],
  updatedDate: new Date(),
  wallet: {
    id: "1",
    balance: "100000",
  },
};

export const workerAccount: Account = {
  id: "1",
  username: "user1",
  email: "user@gmail.com",
  imageUrl: "",
  role: "WORKER",
  isDisable: false,
  isLocked: false,
  addresses: [],
  wallet: {
    id: "1",
    balance: "100000",
  },
  phone: "0123456789",
  createdDate: new Date(),
  updatedDate: new Date(),
};