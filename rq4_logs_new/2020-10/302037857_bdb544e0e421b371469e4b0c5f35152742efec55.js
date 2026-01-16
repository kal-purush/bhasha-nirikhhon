
const livros = require("../model/livros.json");
const fs = require("fs");

const getAllLivro = (req, res) => {
  console.log(req.url);
  res.status(200).send(livros);
};

const getByIdLivro = (req, res) => {
  const id = req.params.id;

  res.status(201).send(livros.find((livro) => livro.id == id));
};

const postLivro = (req, res) =>{
  console.log(req.body);
  const {id, titulo, autor, ano, estoque} = req.body;
  livros.push({id, titulo, autor, ano, estoque});
  
  fs.writeFile("./src/model/livros.json",JSON.stringify(livros),'utf8',function(err){
      if(err){
        return res.status(424).send({message: err});

      }
      console.log("Arquivo atualizado com sucesso!")
  }) 

 res.status(200).send(livros)
};

const deleteLivro = (req, res) => {
  const id = req.params.id;
  const livroFiltrado = livros.find((livro) => livro.id == id);
  const index = livros.indexOf(livroFiltrado);
  livros.splice(index, 1);

  fs.writeFile("./src/model/livros.json",JSON.stringify(livros),'utf8',function(err){
    if(err){
      return res.status(424).send({message: err});
    }
    console.log("Arquivo atualizado com sucesso!")
  })
  res.status(200).send(livros)
  
}

module.exports = {
  getAllLivro,
  getByIdLivro,
  postLivro,
  deleteLivro
};