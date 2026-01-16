// global constant defined in webpack
declare const DEBUG_MODE: boolean;

function isDevelopment(): boolean {
  return DEBUG_MODE;
}

function isProduction(): boolean {
  return !DEBUG_MODE;
}

export default { isDevelopment, isProduction };