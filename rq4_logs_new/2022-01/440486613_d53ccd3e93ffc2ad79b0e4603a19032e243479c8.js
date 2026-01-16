/*
 * Before running this test prepare the environment:
 * - create a .env file based on the "template.env" file
 * - activate the environment by executring the following linux commands:
 *     set -a
 *     source .env
 *     set +a
 * - run the server
 *     npm start
*/

import fetch from 'node-fetch';
import dotenv from 'dotenv';

dotenv.config({ path: new URL('./.env', import.meta.url) });


(async () => {

    if (process.env.MQTT_SUPER_USER && process.env.MQTT_SUPER_PASSWD) {
        const response_00 = await fetch(`http://localhost:${process.env.HTTP_AUTH_PORT}/vmq/lua`, {
            method: 'POST',
            body: JSON.stringify({
                username: process.env.MQTT_SUPER_USER,
                password: process.env.MQTT_SUPER_PASSWD
            }),
            headers: { 'Content-Type': 'application/json' },
        });
        const response_00_json = await response_00.json()
        console.log(response_00_json);
    }

    if (process.env.MQTT_B2B_USER && process.env.MQTT_B2B_PASSWD) {
        const response_01 = await fetch(`http://localhost:${process.env.HTTP_AUTH_PORT}/vmq/lua`, {
            method: 'POST',
            body: JSON.stringify({
                username: process.env.MQTT_B2B_USER,
                password: process.env.MQTT_B2B_PASSWD
            }),
            headers: { 'Content-Type': 'application/json' },
        });
        const response_01_json = await response_01.json()
        console.log(response_01_json);
    }
        
    if (process.env.MQTT_B2C_USER && process.env.MQTT_B2C_PASSWD) {
        const response_02 = await fetch(`http://localhost:${process.env.HTTP_AUTH_PORT}/vmq/lua`, {
            method: 'POST',
            body: JSON.stringify({
                username: process.env.MQTT_B2C_USER,
                password: process.env.MQTT_B2C_PASSWD
            }),
            headers: { 'Content-Type': 'application/json' },
        });
        const response_02_json = await response_02.json()
        console.log(response_02_json);
    }

})();