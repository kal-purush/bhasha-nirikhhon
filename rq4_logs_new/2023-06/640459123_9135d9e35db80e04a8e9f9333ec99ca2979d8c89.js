import { faker } from "@faker-js/faker";
import { v4 as uuid } from "uuid";
import { state as stateArray } from "../utils/Constants";

export function useRandomForm() {
  function getRandomFormData() {
    return {
      id: uuid(),
      name: faker.person.fullName(),
      mobile: faker.phone.number("##########"),
      pincode: faker.location.zipCode("######"),
      city: faker.location.city(),
      address: faker.location.streetAddress(),
      alternateNum: faker.phone.number("##########"),
      state: stateArray[Math.floor(Math.random() * (stateArray.length - 1))],
    };
  }

  return getRandomFormData;
}