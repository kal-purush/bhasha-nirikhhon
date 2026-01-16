import fs from "fs";
import csv from "csv-parser";
import {IPizza, IRawPizzaData} from "@root/runner/mock/types";
import path from "path";
import {transformRawData} from "@root/runner/mock/utils";

const csvDataFilePath = path.join(__dirname, 'pizza_sales.csv');

export function extractCategoryData(): Promise<Set<string>> {
  return new Promise(async (resolve, reject) => {
    const categories = new Set<string>();
    fs.createReadStream(csvDataFilePath)
      .pipe(csv())
      .on('data', async (data: IRawPizzaData) => {
        categories.add(data.pizza_category)
      })
      .on('end', async () => {
        resolve(categories)
      });
  })
}

export async function extractPizzaName(): Promise<Set<string>> {
  return new Promise(async (resolve, reject) => {
    const pizza_names = new Set<string>();
    fs.createReadStream(csvDataFilePath)
      .pipe(csv())
      .on('data', async (data: IRawPizzaData) => {
        pizza_names.add(data.pizza_name)
      })
      .on('end', async () => {
        resolve(pizza_names)
      });
  })
}

export async function extractPizzaData(): Promise<Array<IPizza>> {
  return new Promise(async (resolve, reject) => {
    const pizza_list: Array<IPizza> = [];
    fs.createReadStream(csvDataFilePath)
      .pipe(csv())
      .on('data', async (data: IRawPizzaData) => {
        const formated = transformRawData(data);

        const existed = pizza_list.find((pizza: IPizza) => pizza.name == formated.pizza_name)
        if (!existed) {
          const _pizza: IPizza = {
            name: formated.pizza_name,
            category: formated.pizza_category,
            ingredients: formated.pizza_ingredients,
            sizes: {}
          }

          _pizza.sizes[formated.pizza_size] = formated.unit_price;

          pizza_list.push(_pizza)
        } else {

        }
      })
      .on('end', async () => {
        resolve(pizza_list)
      });
  })
}

extractCategoryData()
.then(data => console.log(data))

extractPizzaName()
.then(data => console.log(data))