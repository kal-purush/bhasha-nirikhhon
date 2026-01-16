import mongoose from "mongoose";
import dotenv from 'dotenv';
dotenv.config();

// die db.ts ist nur um eine Verbindung zur Datenbank herzustellen
// die Datenbank kann ich mit Compass oder mongoAtlas nutzen
// wichtig ist hier der richtige Pfad zur Datenbank

export async function connect(){
    try{
        if(!process.env.DB_HOST || !process.env.DB_NAME){
            throw new Error('Missing environment variables for database connection');
        }
        await mongoose.connect(`mongodb://${process.env.DB_HOST}/${process.env.DB_NAME}`);
        console.log('Connected to the database');

    }catch(error){
        console.error('Error connecting to the database');
        console.error(error);
    }
}