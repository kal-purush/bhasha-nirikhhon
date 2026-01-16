import { IdGenerator } from "../services/IdGenerator";
import { BandaDatabase } from "../data/BandaDatabase";
import { banda,bandaInputDTO } from "../model/banda";
import {  CustomError, InvalidEmail, InvalidName, InvalidPassword } from "../error/customError";

const idGenerator = new IdGenerator()

export class BandaBusiness {
public createBanda = async (input: bandaInputDTO) => {
   try {
     const { name, music_genre, responsible } = input;
     

     if (!name || !music_genre  || !responsible) {
        throw new CustomError(400,
          'Preencha os campos "name","music_genre" , "responsible" ');
      }
     const id: string = idGenerator.generate()
     const bandaDatabase = new BandaDatabase();
    const banda:banda = {
      id,
      name,
      music_genre,
      responsible
    }

     await bandaDatabase.insertBanda({
       id,
       name,
       music_genre,
       responsible
 
     });
    } catch (error:any) {
     throw new Error(error.message);
   }
 }

public getBanda = async (banda:banda) => {
  try {

      return await new BandaDatabase().getBanda(banda);
     
    } catch (error:any) {
    throw new Error(error.message);
  }
}
public removeBanda = async (banda:banda) => {
  try {
      
      return await new BandaDatabase().removeBanda(banda);
  
   
    } catch (error:any) {
    throw new Error(error.message);
  }
}
}
