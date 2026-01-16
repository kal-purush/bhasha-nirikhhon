import {friend} from "../model/singup"
import { FriendDatabase } from "../data/FriendDatabase";


export class FriendBusiness {
public createFriend = async (input: any) => {
   try {
     const { id, friendId } = input;
 
     if ( !friendId ) {
       throw new Error(
         'Preencha os campos "o id do amigo "'
       );
     }
 
 
     const friendDatabase = new FriendDatabase();
     await friendDatabase.insertFriend({id, friendId});

   } catch (error: any) {
     throw new Error(error.message);
   }
 }
 public removeFriend = async (friend:friend) => {
  try {
      
      return await new FriendDatabase().removeFriend(friend);
  
   
  } catch (error: any) {
    throw new Error(error.message);
  }
}
}
