const fs = require('fs');
const path = require('path');

function deleteFilesWithExtensions(directory, extensions) {
  // Lê o conteúdo do diretório
  fs.readdir(directory, (err, files) => {
    if (err) {
      console.error(`Erro ao ler o diretório ${directory}: ${err}`);
      return;
    }

    // Itera sobre os arquivos encontrados
    files.forEach((file) => {
      const filePath = path.join(directory, file);

      // Verifica se o caminho é um diretório
      fs.stat(filePath, (err, stat) => {
        if (err) {
          console.error(`Erro ao obter informações do arquivo ${filePath}: ${err}`);
          return;
        }

        if (stat.isDirectory()) {
          // Se for um diretório, chama a função recursivamente para percorrer suas subpastas
          deleteFilesWithExtensions(filePath, extensions);
        } else {
          // Verifica se o arquivo possui uma das extensões especificadas
          if (extensions.some((extension) => file.endsWith(extension))) {
            // Exclui o arquivo
            fs.unlink(filePath, (err) => {
              if (err) {
                console.error(`Erro ao excluir o arquivo ${filePath}: ${err}`);
              } else {
                console.log(`Arquivo excluído: ${filePath}`);
              }
            });
          }
        }
      });
    });
  });
}

// Define o diretório inicial e as extensões a serem excluídas
const directory = './src';
const extensionsToDelete = ['.css', '.css.map'];

// Chama a função para excluir os arquivos
deleteFilesWithExtensions(directory, extensionsToDelete);