import Logger from "./lib/Logger";

export function logger (){
  return new Logger();
}

export {middleware} from "./lib/Middleware";