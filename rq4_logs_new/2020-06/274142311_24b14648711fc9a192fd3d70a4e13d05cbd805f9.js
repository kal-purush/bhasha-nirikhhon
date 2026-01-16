//transpilleur --> compilation du code : babel 

//import d'express
import express from 'express';
import bodyParser from 'body-parser';
import cors from 'cors';
import router from './src/routes/route'
import database from './src/models/database'

//creation du serveur -- de l'app
const app = express();

//configuration du serveur avec Cors et BodyParser
	//Installation Cors			OBLIGATOIRE		multirequetage 	npm install cors 
	//Installation BodyParser 	OBLIGATOIRE						npm install body-parser

app.use(bodyParser.urlencoded({extended: false}));
app.use(bodyParser.json());
app.use(cors({origin: true}));
app.use('/',router)

//Lancement du serveur
const port = 3004;

database().then(async()=>{
	console.log('Database server is connected');
	app.listen(port,()=>{
	console.log(`Serveur lancé sur le port ${port}...`);
});
})

export default app;