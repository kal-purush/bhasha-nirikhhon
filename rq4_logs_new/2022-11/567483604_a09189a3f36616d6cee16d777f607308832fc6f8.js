import express, { json } from "express"; // instalamos express que nos ayudara a ejecutar un servidor
import indexrutas from "./routes/index.routes.js" // importamos esta ruta index que nos ayuda a saber si tenemos conexion a BD
import consultas from "./routes/consulta.routes.js" // importamos todos las rutas de nuestra consulta 
import { PORT } from "./config.js";



const app= express();

app.use(express.json());

app.use(indexrutas);
app.use(consultas);

app.listen(PORT);

// nos muestra el numero de puerto con el que estamos conectado
console.log(PORT)