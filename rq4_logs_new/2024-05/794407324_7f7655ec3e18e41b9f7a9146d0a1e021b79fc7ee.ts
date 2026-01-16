import inquirer from "inquirer";
import { IAnimal, QAnimal } from "../interfaces/IAnimal.js";
import { Cat } from "../models/Cat.js";
import { Dog } from "../models/Dog.js";
import { QAnimalName, QAnimalType } from "../questions/QAnimal.js";

export class AnimalFactory {
    static async createAnimal(): Promise<IAnimal> {
        const { animalName, animalType } = await inquirer.prompt([QAnimalType,QAnimalName]) as QAnimal
        if (animalType === "Gato") {
            return new Cat(animalName)
        }

        if (animalType === "Cachorro") {
            return new Dog(animalName)
        }

        throw new Error("Tipo de animal não suportado!")
    }
}