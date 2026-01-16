#!/usr/bin/env node
//@ts-check

const got = require("got");
const mqttusvc = require("mqtt-usvc");

/**
 * @typedef {object} Device
 * @property {string} id
 * @property {string} host
 */

/**
 * @typedef {object} Config
 * @property {Device[]} devices
 * @property {number} poll_interval
 */

/** @typedef {import("got").Options} Options */

/** @type {Options} */
const gotOpts = {
  https: { rejectUnauthorized: false },
  responseType: "text",
};

const getUrl = (host, command, params = []) =>
  `https://${host}/httpapi.asp?command=${[command, ...params].join(":")}`;

async function main() {
  const service = await mqttusvc.create();
  /** @type {Config} */
  const config = service.config;

  const execCmd = async (device, command, params = []) => {
    const url = getUrl(device.host, command, params);
    const res = await got(url, gotOpts);
    console.info(
      `Executed ${[command, ...params].join(":")} on ${device.host}`
    );

    const body = await res.body;

    if (command === "getPlayerStatus") {
      service.send(`~/status/${device.id}/playerStatus`, JSON.parse(body));
    } else if (command === "getStatusEx") {
      service.send(`~/status/${device.id}/statusEx`, JSON.parse(body));
    } else if (command === "wlanGetConnectState") {
      service.send(`~/status/${device.id}/wlanConnectState`, body);
    } else if (command === "EQGetStat") {
      service.send(`~/status/${device.id}/EQStat`, JSON.parse(body));
    } else if (command === "EQGetList") {
      service.send(`~/status/${device.id}/EQList`, JSON.parse(body));
    } else if (command === "getShutdown") {
      service.send(`~/status/${device.id}/shutdown`, body);
    } else if (command === "getAlarmClock") {
      service.send(`~/status/${device.id}/alarmClock`, JSON.parse(body));
    } else if (command === "setPlayerCmd") {
      setTimeout(() => execCmd(device, "getPlayerStatus"), 100);
    }
  };

  setInterval(async () => {
    for (const device of config.devices) {
      execCmd(device, "getPlayerStatus", []);
    }
  }, config.poll_interval);

  service.on("message", (topic, data) => {
    const [, action, id, command] = topic.split("/");

    if (action !== "run") return;

    const device = config.devices.find((d) => d.id === id);
    if (!device) return;

    let params = data || [];

    execCmd(device, command, params)
      //   .then(() => {
      //     console.log("OK -> " + message);
      //   })
      .catch((err) => console.error("error -> %s", err.stack));
  });

  config.devices.forEach((d) => {
    service.subscribe("~/run/" + d.id + "/#");
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});