import React from "react";

type User = {
  id: number;
  email: string;
  first_name: string;
  last_name: string;
  avatar: string;
  age: number;
};
type Users = Array<User>;

const users: Users = [
  {
    id: 7,
    email: "michael.lawson@reqres.in",
    first_name: "Michael",
    last_name: "Lawson",
    avatar: "https://reqres.in/img/faces/7-image.jpg",
    age: 23,
  },
  {
    id: 8,
    email: "lindsay.ferguson@reqres.in",
    first_name: "Lindsay",
    last_name: "Ferguson",
    avatar: "https://reqres.in/img/faces/8-image.jpg",
    age: 20,
  },
];

type Product = {
  id: number;
  name: string;
  price: number;
  currency: string;
  ingredients: string[];
  type: string;
  available: boolean;
};
type Products = Array<Product>;

const products: Products = [
  {
    id: 1,
    name: "Burger Premium",
    price: 6,
    currency: "euro",
    ingredients: ["flour", "beef", "salad", "cheese", "sauce"],
    type: "burger",
    available: true,
  },
  {
    id: 2,
    name: "Burger Lite",
    price: 2.3,
    currency: "euro",
    ingredients: ["flour", "beef", "cheese", "sauce", "cucumber"],
    type: "burger",
    available: true,
  },
];
type GetUser = (id: number, users: Users) => User | undefined;

const getUser: GetUser = (id, users) => users.find((user) => user.id === id);

// С ниже приведенным массивом решить следующие задачи. Все функции и данные должны быть протипизированы:

//     1. Создать строку из названий стран через запятую
//     2. Посчитать общее количнство людей в данном массиве стран.
//     3. Создать функцию, которая бы принимала массив стран и сортировала бы их по названию.
//     4. Получить массив валют.
//     5. Получить массив городов, отсортированных в алвавитном порядке.
//     6. Создать функцию, которая бы принимала массив стран и отдавала бы среднее количество людей в этих странах.

type Country = {
  country: string;
  abbreviation: string;
  city: string;
  currency_name: string;
  population: number;
};

type Countries = Array<Country>;

const countries: Countries = [
  {
    country: "United Arab Emirates",
    abbreviation: "AE",
    city: "Abu Dhabi",
    currency_name: "Arab Emirates Dirham",
    population: 9630959,
  },
  {
    country: "Poland",
    abbreviation: "PL",
    city: "Warszawa",
    currency_name: "Polish Zloty",
    population: 37974750,
  },
  {
    country: "Russian Federation",
    abbreviation: "RU",
    city: "Moscow",
    currency_name: "Russian Ruble",
    population: 144478050,
  },
];

//     1. Создать строку из названий стран через запятую

type GetCountry = (item: Array<Country>) => string;

const getCountry: GetCountry = (countries) => {
  let allCont = "";
  countries.forEach((i) => {
    allCont += `${i.country}, `;
  });
  return allCont;
};
//  export default getCountry

//     2. Посчитать общее количнство людей в данном массиве стран.

type GetNumberPeople = (item: Array<Country>) => number;

const getNumberPeople: GetNumberPeople = (countries) => {
  let allPeople = 0;
  countries.forEach((i) => {
    allPeople += i.population;
  });
  return allPeople;
};
//  export default getNumberPeople

//     3. Создать функцию, которая бы принимала массив стран и сортировала бы их по названию.

type SortCountries = (item: Array<Country>) => void;

const sortCountries: SortCountries = (countries) => {
  countries.sort((a, b) => (a.country > b.country ? 1 : -1));
  return countries;
};
//  export default sortCountries

//     4. Получить массив валют.


type GetCurrencyName = (item: Array<Country>) => Array<string>;


const getCurrencyName: GetCurrencyName = (countries) => {

  let allCurrencyName: string[] = []; // почему так?

  countries.forEach((i) => {

    allCurrencyName.push(i.currency_name);

  });
 
  return allCurrencyName;

};
//  export default getCurrencyName

//     5. Получить массив городов, отсортированных в алвавитном порядке.

type SortCities = (item: Array<Country>) => void;

const sortCities: SortCities = (countries) => {
  countries.sort((a, b) => (a.city > b.city ? 1 : -1));
  return countries;
};
//  export default sortCities

//     6. Создать функцию, которая бы принимала массив стран и отдавала бы среднее количество людей в этих странах.


type GetPopulation = (item: Array<Country>) => string;

const getPopulation: GetPopulation = (countries) => {
  let allPop = "";
  countries.forEach((i) => {
    allPop += `In ${i.country} population is ${i.population} people. `;
  });
  return allPop;
};
 export default getPopulation