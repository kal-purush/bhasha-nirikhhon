const rcedit = require('rcedit');
const path = require('path');

const executablePath = path.resolve('dist/API.exe'); 
const iconPath = path.resolve('icon.ico'); 

//node build.js

rcedit(executablePath, { icon: iconPath }, (error) => {
  if (error) {
    console.error('Erro ao adicionar o ícone ao executável:', error);
  } else {
    console.log('Ícone adicionado com sucesso ao executável.');
  }
});