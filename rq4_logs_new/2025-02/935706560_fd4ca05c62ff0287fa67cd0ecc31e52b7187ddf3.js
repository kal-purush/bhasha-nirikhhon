import express from 'express';
import bodyParser from 'body-parser'
import axios from 'axios';

import jwt from "jsonwebtoken";
import cors from "cors";
import cookieParser from 'cookie-parser';

const app = express();
const port = 3000;

app.use(express.json());
app.use(express.static('public', {
    index: false
}));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(cors());
app.use(cookieParser())
const API_SOAP = "http://localhost:8000/eventos/";
const API_REST = "http://localhost:8080/users/";
const SECRET_KEY = "minhachavemuitosegurasegurademaisdescobreai"

// rotas
// Middleware para verificar o token
const authenticateToken = (req, res, next) => {
    const token = req.cookies.token;
  
    if (!token) return res.redirect("/login");
  
    try {
      const decoded = jwt.verify(token, SECRET_KEY);
      req.user = decoded;
      next();
    } catch (error) {
      res.redirect("/login");
    }
};


// home
app.get("/", authenticateToken, (req, res) => {
    res.sendFile("index.html", { root: 'public' });
})

// eventos
app.get("/ranking", authenticateToken, (req, res) => {
    res.sendFile('ranking.html', { root: 'public' });
})

// enviar username para o SOAP
app.post("/tentativa", authenticateToken, async (req, res) => {
    const { palavra } = req.body;    
    const username = req.user.username;

    try {
        // aumentando o score do usuário
        const user = (await axios.get(`${API_REST+username}`)).data; // get use by username from rest api
        const score = parseInt(user.score) + 1;
        const userToPatch = {
            score: score
        }

        await axios.patch(`${API_REST}${user.id}`, userToPatch, {
            headers: {
                "Content-Type": "application/json"
            }
        }); // get use by username from rest api

        const xmlData = `
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:soap="http://src.soap/">
            <soapenv:Header/>
            <soapenv:Body>
                <soap:mensagem>
                    <arg0>${username}</arg0>
                    <arg1>${palavra}</arg1>
                </soap:mensagem>
            </soapenv:Body>
        </soapenv:Envelope>
        `;

        await axios.post(API_SOAP, xmlData, {
            headers: {
                'Content-Type': 'text/xml',
            }
        })
        console.log("Tentativa enviada com sucesso!");
        
    } catch (err) {
        console.log(err);
    }

    // res.redirect('/eventos');
})

// cadastro render file
app.get("/cadastro", (req, res) => {
    res.sendFile('cadastro.html', { root: 'public' });
})

// cadastra um novo usuário pela API Rest
app.post("/cadastro", async (req, res) => {
    const { username, password } = req.body;
    const dados = {
        username,
        password
    }

    try {
        const response = await axios.post(`${API_REST}`, dados, {
            headers: {
                "Content-Type": "application/json"
            }
        })
        console.log(response.data);
        res.redirect('/login');
    } catch (err){
        res.redirect("/error");
        console.log("An erro has ocurred while post user: ", err.response);
    }
})

// login
app.get("/login", (req, res) => {    
    res.sendFile('login.html', { root: 'public' });
})

// login
app.post("/login", async (req, res) => {    
    const { username, password } = req.body;
    try {
        const user = (await axios.get(`${API_REST+username}`)).data; // get use by username from rest api
        console.log(user);
        
        if(!user || password != user.password)
            // res.redirect("/error");
            return res.status(401).json({ error: "Credenciais inválidas" });

        const token = jwt.sign({ username }, SECRET_KEY, { expiresIn: '1h' });
        res.cookie("token", token, { httpOnly: true, maxAge: 3600000 });
        res.redirect("/");
    } catch (err) {
        res.redirect("/error");
        console.log("An erro has ocurred while post user: ", err.response);
    }
    res.sendFile('login.html', { root: 'public' });
})

// Logout (Remover Token)
app.get("/logout", (req, res) => {
    res.clearCookie("token");
    res.redirect("/login");
});


app.get("/error", (req, res) => {
    res.sendFile('error.html', { root: 'public' });
})

app.listen(port,'0.0.0.0', () => {
    console.log(`App is running at port ${port}...`);
})