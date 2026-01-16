import { MarketSpeed, InputType } from 'rakuten-auto-login';
const account = require('../../config/config.json').account;


const input: InputType = {
  user: account.id,
  password: account.pass,
  version: '15.4',
  dir: 'C:/Program Files (x86)/MarketSpeed/MarketSpeed',
  filename: 'MarketSpeed.exe'
}
const marketSpeed = new MarketSpeed(input);
marketSpeed.login().then(res => console.log('marketSpeed login:', res));