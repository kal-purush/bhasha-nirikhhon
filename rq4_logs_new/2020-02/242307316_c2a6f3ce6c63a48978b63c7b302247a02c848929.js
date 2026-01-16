#!/usr/bin/env node

// https://github.com/yargs/yargs
const { showHelp } = require('yargs');
// https://github.com/chalk/chalk
// https://alligator.io/nodejs/styling-output-command-line-node-scripts-chalk/
const chalk = require('chalk');
// https://github.com/visionmedia/debug
const debug = require('debug')('ldap-passwd:bin');

const { checkPassword, hashPassword } = require('../index');

const argv = require('yargs')
  .usage('Usage: $0 <command> [options]')
  .command('verify', 'Verify the password and the hashed password is match')
  .example('$0 verify -p plain_password -c hash_to_check', 'Verify password')
  .command('generate', 'Generate the hashed password from the plain password')
  .example('$0 generate -p plain_password', 'Generate password')
  .option('password', {
    alias: 'p',
    type: 'string',
    description: 'Plain password for verify or generate',
    default: null,
  })
  .option('check_hash', {
    alias: 'c',
    type: 'string',
    description: 'Hashed password for verifying...',
    default: null,
  })
  .option('encoder', {
    alias: 'e',
    type: 'string',
    description: 'The encoder for hash, like md5, sha1, sha256, ...',
    default: 'sha1',
  })
  .option('suffixed', {
    type: 'boolean',
    description: 'Wether the salt prefixed or suffixed',
    default: true,
  })
  .option('salt', {
    alias: 's',
    type: 'string',
    description: 'Salt, 8 bytes string or 12-length base64, abcdefgh or YWJjZGVkZmg= with -b',
    default: null,
  })
  .option('salt_size', {
    type: 'number',
    description: 'salt length in hex format, equals to 8 bytes or 12-length base64',
    default: 16,
  })
  .option('salt_base64_encoded', {
    alias: 'b',
    type: 'boolean',
    description: 'Wether the salt is base64 encoded',
    default: false,
  })
  .help('h')
  .alias('h', 'help')
  .alias('v', 'version')
  .epilog('Copyright (c) 2020 liudonghua').argv;

debug(`argv: ${JSON.stringify(argv, null, 2)}`);

let { password, check_hash, salt, salt_size, suffixed, encoder, salt_base64_encoded } = argv;
const [command] = argv._;
switch (command) {
  case 'verify':
    debug(`do verifying...`);
    const is_valid = checkPassword(password, check_hash, salt_size, suffixed) === true;
    console.info(
      `verify result: ${is_valid ? chalk.bgGreen.blue.bold(is_valid) : chalk.bgRed.yellow.italic(is_valid)}`
    );

    break;
  case 'generate':
    debug(`do generate...`);
    salt = salt ? Buffer.from(salt, salt_base64_encoded ? 'base64' : 'utf8').toString('hex') : null;
    hash_password = hashPassword(encoder, password, salt, salt_size, suffixed);
    console.info(chalk.bgGreen.blue.bold(hash_password));
    break;
  default:
    debug(`command should be specified as verify or generate, you provided ${command}`);
    showHelp();
    process.exit(1);
}