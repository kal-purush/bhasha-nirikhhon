
import { env as devEnv } from './environment';

export const env = Object.assign({}, devEnv);
env.production = true;