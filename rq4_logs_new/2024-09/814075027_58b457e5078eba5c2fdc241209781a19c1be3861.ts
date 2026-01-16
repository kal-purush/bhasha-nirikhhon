import { Value } from "@sinclair/typebox/value";
import { Env, envConfigValidator, startStopSchema, StartStopSettings, startStopSettingsValidator } from "../types";

export function validateAndDecodeSchemas(env: Env, rawSettings: object) {
  const settings = Value.Default(startStopSchema, rawSettings) as StartStopSettings;

  if (!startStopSettingsValidator.test(settings)) {
    const errorDetails: object[] = [];
    for (const error of startStopSettingsValidator.errors(settings)) {
      const errorMessage = { path: error.path, message: error.message, value: error.value };
      console.error(errorMessage);
      errorDetails.push(errorMessage);
    }
    return new Response(JSON.stringify({ message: `Bad Request: the settings are invalid.`, errors: errorDetails }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  const decodedSettings = Value.Decode(startStopSchema, settings);

  if (!envConfigValidator.test(env)) {
    const errorDetails: object[] = [];
    for (const error of envConfigValidator.errors(env)) {
      const errorMessage = { path: error.path, message: error.message, value: error.value };
      console.error(errorMessage);
      errorDetails.push(errorMessage);
    }
    return new Response(JSON.stringify({ message: `Bad Request: the environment is invalid.`, errors: errorDetails }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  const decodedEnv = Value.Decode(envConfigValidator.schema, env);

  return { decodedEnv, decodedSettings };
}