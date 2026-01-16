import * as dotenv from 'dotenv';

dotenv.config();

// Provide data from .env file

const DATABASE_CONSTRING = process.env.DATABASE_CONSTRING ?? '';
const TOKEN_KEY = process.env.TOKEN_KEY ?? '';

export default {
    DATABASE_CONSTRING,
    TOKEN_KEY
}