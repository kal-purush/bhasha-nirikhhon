import * as fs from 'fs';

class Config {
  discordToken: string = "";

  constructor() {
    this.load();
  }

  load() {
    const configValues = JSON.parse(fs.readFileSync('config/config.json', 'utf8'));

    this.discordToken = this.parse(configValues, 'discordToken');
  }

  parse(values: { [index:string]: string }, key: string): string {

    if (!values.hasOwnProperty(key)) {
      throw `'${key}' not found in config.json!`;
    }

    return values[key];
  }
}

export default Config;