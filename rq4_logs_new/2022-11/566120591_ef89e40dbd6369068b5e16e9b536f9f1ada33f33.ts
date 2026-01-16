import * as dotenv from "dotenv";
dotenv.config();

import { Endpoint, IncomingMessage } from "@domotics-app/lib";
import { Component, Server } from "@resources";

const BOARD_LEDS = new Component(20, "low");
const FRONT_LEDS = new Component(21, "low");
const BACK_LEDS = new Component(26, "low");

const PRINCIPAL_CURTAIN = new Component(5, "low");
const SECONDARY_CURTAIN = new Component(6, "low");

const DESKTOP = new Component(23, "low");
const PROJECTOR = new Component(24, "low");

const PORT = Number(process.env.PORT) || 3000;
const server = new Server(PORT);

const changeMessageHandler = (message: IncomingMessage) => {
  switch (message.endpoint) {
    case Endpoint.ALL_LIGHTS_ARE_ON:
      return Promise.all([
        BOARD_LEDS.write(message.payload),
        FRONT_LEDS.write(message.payload),
        BACK_LEDS.write(message.payload),
      ]);

    case Endpoint.BACK_LIGHTS_ARE_ON:
      return BACK_LEDS.write(message.payload);

    case Endpoint.BOARD_LIGHTS_ARE_ON:
      return BOARD_LEDS.write(message.payload);

    case Endpoint.DESKTOP_IS_ON:
      return DESKTOP.write(message.payload);

    case Endpoint.FIRST_CURTAIN_IS_OPEN:
      return PRINCIPAL_CURTAIN.write(message.payload);

    case Endpoint.SECOND_CURTAIN_IS_OPEN:
      return SECONDARY_CURTAIN.write(message.payload);

    case Endpoint.PROJECTOR_IS_ON:
      return PROJECTOR.write(message.payload);
  }
};

server.on("CHANGE", (message) => {
  changeMessageHandler(message)
    ?.then(() => {
      server.emit("CHANGE", {
        endpoint: message.endpoint,
        payload: message.payload,
      });
    })
    .catch((err) => console.error(err));
});

server.run();

process.on("SIGINT", () => {
  FRONT_LEDS.unexport();
  BACK_LEDS.unexport();
  BOARD_LEDS.unexport();

  PRINCIPAL_CURTAIN.unexport();
  SECONDARY_CURTAIN.unexport();

  DESKTOP.unexport();
  PROJECTOR.unexport();

  process.exit();
});