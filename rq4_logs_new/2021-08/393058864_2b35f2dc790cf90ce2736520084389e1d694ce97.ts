#!/usr/bin/env node

import prompts from 'prompts';

import reverseLookup from './lookup';

async function prompt() {
  const promptresult = await prompts({
    type: 'text',
    name: 'number',
    validate,
    message: 'Telephone Number:'
  });
  const number = promptresult.number;
  const reverseLookupResult = await reverseLookup(number);
  return console.log(reverseLookupResult);
}

function validate(number: string) {
  const realNumber = Number.parseInt(number);
  if (Number.isNaN(realNumber)) {
    return 'Not a valid Telephone Number';
  } else {
    return true;
  }
}

prompt();