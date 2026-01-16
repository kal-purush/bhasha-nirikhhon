import prompt from "prompt";
import {
  addEventChannel,
  addUpdateChannel,
} from "./db.js";

const schema = {
  properties: {
    typeOf: {
      description: "Type of channel: (1) EventChannel  or (2) UpdateChannel",
      type: "integer",
      required: true,
    },
    channel: {
      description: "ChannelId",
      type: "string",
      required: true,
    }
  }
}
prompt.start();

prompt.get(schema, (err, result) => {
  if (err) return;
  switch(result.typeOf) {
    case 1:
      addEventChannel(result.channel);
      break
    case 2:
      addUpdateChannel(result.channel);
      break
    default:
      console.error("invalid channel type");
  }
});