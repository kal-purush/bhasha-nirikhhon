import { Property } from "../entities/property";
import { generateNumberByInterval } from "./utils-functions";

export default class PropertiesFunctions {
    static createProperties(): Property[] {
        let properties: Property[] = []; 
        for(let i = 0; i < 20; i++){
            let property: Property = {
                saleValue: generateNumberByInterval(200, 1200),
                rentValue: generateNumberByInterval(20, 120),
                owner: null
            }
            properties.push(property)
        }
        return properties;
    }
}