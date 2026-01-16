import { existsSync, readFileSync } from 'fs';
import { resolve } from 'path';
import { fromPairs } from 'ramda';

const ENV_PATH = resolve(__dirname, '..', '.env');

export default function readEnvFile() {
  if (existsSync(ENV_PATH)) {
    const content = readFileSync(ENV_PATH).toString();
    const obj = fromPairs(
      content.split('\n').map(s => s.split('=') as [string, string])
    );
    Object.assign(process.env, obj);
  } else {
    console.warn(ENV_PATH, "doesn't exist.");
  }
}