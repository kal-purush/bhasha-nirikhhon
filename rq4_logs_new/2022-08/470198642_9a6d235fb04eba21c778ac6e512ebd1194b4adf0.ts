import {execSync} from 'child_process';

import * as dotenv from 'dotenv';
dotenv.config({ path: `../../.env.test`});

export default async function before (){
    execSync('docker-compose up -d db && yarn db:up')
}