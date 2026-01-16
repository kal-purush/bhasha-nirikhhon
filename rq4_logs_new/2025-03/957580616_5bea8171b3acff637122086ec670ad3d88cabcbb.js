const express      = require('express');
const app          = express();
const db           = require('./db/connection.js');
const bodyParser   = require('body-parser');
const pessoaRoutes = require('./routes/pessoa.js'); 

const PORT = 3000;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.listen(PORT, function() {
    console.log(`O express está rodando na porta ${PORT}`);
});

// db connection

db.authenticate()
    .then(() => {
        console.log('Conectou ao banco com sucesso!');
    })
    .catch(err => {
        console.log('Ocorreu um erro ao conectar", err');
    });

// boot route
app.get('/', (req, res) => {
    res.send('apiPessoas está funcionando!');
});

// person route
app.use('/', pessoaRoutes); 