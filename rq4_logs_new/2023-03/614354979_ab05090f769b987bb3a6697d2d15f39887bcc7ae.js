const express = require('express')
const app = express()

app.use(express.json()) //faz com que o express entenda o formato json


app.get('/', (req, res) => {//rota raiz
  res.send('Hello World!')
  
}) //cria uma rota raiz

app.get('/contato/:id', (req, res) => {

  const{id} = req.params//pega o id da url, desestruturação de objeto. 
  const {sit} = req.query //pega o parametro sit da url
  
  //aqui vai ser o codigo para retornar uma resposta no formato json
  return res.json(
    {
    id, // valido somente se o nome da variavel for igual ao nome da propriedade
    cidade : sit,
    "contato 1": "email1", 
    "contato 2": "email2"})
    
  }) //cria uma rota raiz


  app.post('/contato', (req, res) => {
    
    const {nome, email} = req.body //pega o nome e o email do corpo da requisição

    //aqui vai ser o codigo para retornar uma resposta no formato json
    return res.json(
      {
      nome, // valido somente se o nome da variavel for igual ao nome da propriedade
      email : email,
      "contato 1": "email1", 
      "contato 2": "email2"})
      
    }) 

  
  app.listen(3000, () => {
    console.log('Example app listening on port 3000!')
  }) //roda o servidor na porta 3000