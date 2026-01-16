#!/usr/bin/env node
var program = require('commander');

program
  .version('0.0.1')
  .command('add', 'Adicionar arquivos para commitar')
  .command('annotate', 'cvs annotate')
  .command('checkout', 'Checkout em um ou mais arquivo(s)')
  .command('config', 'Exibe configurações da aplicação')
  .command('die', 'Remove o(s) arquivo(s) commitado(s) no CVS')
  .command('diff', 'Exibe diferenças entre versões do arquivo')
  .command('history', 'Exibe histórico do repositorio')
  .command('init', 'Inicializa diretório atual')
  .command('log', 'Exibe log dos commits de um arquivo')
  .command('pull', 'Baixa atualizações do repositorio')
  .command('push', 'Envia modificações para repositório')
  .command('rm', 'Remove arquivos da lista de commit')
  .command('status', 'Lista diferenças com o repositorio')
  .command('tag', 'Altera tag dos arquivos já adicionados para commit(comando cvsgit add)')
  .command('whatchanged', 'what changed')
  .parse(process.argv);