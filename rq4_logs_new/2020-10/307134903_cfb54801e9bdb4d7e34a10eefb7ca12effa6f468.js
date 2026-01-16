/* eslint-disable require-await */
import { v4 as uuidv4 } from 'uuid';

let cars = [
  {
    id: 1,
    make: 'Ford',
    year: '2015',
  },
  {
    id: 2,
    make: 'Honda',
    year: '2010',
  },
];

export const getAllCars = async () => {
  return JSON.parse(JSON.stringify(cars));
};

export const createCar = async (data) => {
  const newCar = { id: uuidv4(), ...data };
  cars.push(newCar);
  return newCar;
};

export const findCarById = async (id) => {
  return cars.filter((car) => car.id === id)[0];
};