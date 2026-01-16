import * as Joi from "joi";
import { is } from "../../util";

interface BotResponse {
  id: string;
  error?: void;
  result: {
    [key: string]: any;
    method: string;
  };
};

let goodResponseValidator = Joi.object().keys({
  id: Joi.string(),
  result: Joi.object(),
  error: Joi.any().allow(null, false)
});

// Type guard function to do type checking at runtime.
let isBotResponse = is<BotResponse>(goodResponseValidator);

export interface RPCError {
  error: string;
  method: string;
}

export interface BotErrorResponse {
  id: string;
  result?: void;
  error: RPCError;
};

let botErrorResponseValidator = Joi.object().keys({
  id: Joi.string(),
  error: Joi.object({method: Joi.string(), error: Joi.string()}),
  result: Joi.any().allow(null, false)
});

// Type guard function to do type checking at runtime.
let isBotErrorResponse = is<BotErrorResponse>(botErrorResponseValidator);

interface Notification {
  id: void;
  result: {
    [key: string]: any;
  };
};

let notificationValidator = Joi.object().keys({
  id: Joi.any().allow(null, false),
  result: Joi.object()
});

let isNotification = is<Notification>(notificationValidator);

interface ResponseOutcome {
  response: (r: BotResponse) => any;
  error: (r: BotErrorResponse) => any;
  notification: (r: Notification) => any;
  _: (r: any) => any;
}

export let catchMessage = (resp) => (choices: ResponseOutcome) => {
  let results = _({
      response: isBotResponse,
      error: isBotErrorResponse,
      notification: isNotification
    }).mapValues((f: Function) => f(resp));
  let matchCount = (results.invert(true).value())["true"] || [];
  let isAmbiguous = matchCount.length > 1;
  let result = (isAmbiguous ? "_" : results.findKey((v) => !!v)) || "_";
  console.log(result);
  console.dir(resp);
  return result;
};