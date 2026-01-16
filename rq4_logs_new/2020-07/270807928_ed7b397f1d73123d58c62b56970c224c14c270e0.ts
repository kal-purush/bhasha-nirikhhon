import { getAllUsers, getUserById, saveOneUser } from "../daos/user-dao";
import { User } from "../models/User";




//the most basic service function you will see
//all it does is call the dao
// its easier to expand a function that already exists instead of inserting a new function in to the mix
export async function getAllUsersService():Promise<User[]>{
    return await getAllUsers()
}


export async function getUserByIDService(id:number):Promise<User>{
    return await getUserById(id)
}

export async function saveOneUserService( newUser:User):Promise<User>{
    return await saveOneUser(newUser)
}