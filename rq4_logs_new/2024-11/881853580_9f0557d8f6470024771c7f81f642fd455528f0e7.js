const express = require('express');
const bodyParser = require('body-parser');
const nodemailer = require('nodemailer');
const mongoose = require('mongoose');

const app = express();
app.use(bodyParser.json());

// Conexão com o MongoDB
mongoose.connect('mongodb://localhost:27017/leadsDB', { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Conectado ao MongoDB'))
  .catch(err => console.log(err));

// Modelo de Lead
const Lead = mongoose.model('Lead', new mongoose.Schema({
  name: String,
  email: String
}));

// Rota para receber os dados do formulário
app.post('/submit', async (req, res) => {
  const { name, email } = req.body;

  // Criar um novo lead no MongoDB
  const lead = new Lead({ name, email });
  await lead.save();

  // Configuração do Nodemailer para enviar e-mail
  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: 'seuemail@gmail.com',
      pass: 'suasenha'
    }
  });

  const mailOptions = {
    from: 'seuemail@gmail.com',
    to: email,
    subject: 'Bem-vindo! Aqui está o seu e-book.',
    text: `Olá ${name}, obrigado por se inscrever! Aqui está o link para baixar o e-book: https://exemplo.com/ebook`
  };

  // Enviar e-mail
  transporter.sendMail(mailOptions, function(error, info){
    if (error) {
      console.log(error);
      res.status(500).send('Erro ao enviar o e-mail.');
    } else {
      console.log('Email enviado: ' + info.response);
      res.status(200).send('E-mail enviado com sucesso!');
    }
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});