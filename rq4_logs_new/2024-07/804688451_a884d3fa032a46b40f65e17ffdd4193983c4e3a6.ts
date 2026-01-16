import dotenv from 'dotenv';

// Cargar las variables de entorno desde el archivo .env
dotenv.config();

// Obtener las variables de entorno
function getEnvironmentVariable(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} must be set.`);
  }
  return value;
}

// Exportar las configuraciones de la base de datos
export const dbConfig = {
  user: getEnvironmentVariable('DB_USER'),
  host: getEnvironmentVariable('DB_HOST'),
  database: getEnvironmentVariable('DB_DATABASE'),
  password: getEnvironmentVariable('DB_PASSWORD'),
 
};

// Exportar otras configuraciones si es necesario
