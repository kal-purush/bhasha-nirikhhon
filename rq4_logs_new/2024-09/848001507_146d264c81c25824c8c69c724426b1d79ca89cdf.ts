import fs from "fs"
import Trip from "./Trip/Trip.js"
import User from "./Auth/User.js";

interface UserMap {[key: string]: User}
interface TripMap {[key: string]: Trip}
const users: UserMap = {};
const trips: TripMap = {}; 
const DATABASE_PATH = './database.json'
export const update = async (users: UserMap, trips: TripMap) => {
    await fs.writeFile(DATABASE_PATH,JSON.stringify({
      users,
      trips
    }), err => {
      if (err) throw new Error("Failed to write to database");
    });
} 



export const save = () => update(users,trips);

export const reset = () => {
  update({},{});  
}