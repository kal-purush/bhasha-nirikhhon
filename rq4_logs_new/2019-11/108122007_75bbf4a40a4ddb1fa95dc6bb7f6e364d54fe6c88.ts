import { Get, Controller } from "@nestjs/common";

@Controller("env")
export class EnvController {

  @Get('/')
  getEnvVariables(): any {
    let res = {};
    for (const key in process.env) {
      if (process.env.hasOwnProperty(key)) {
        const element = process.env[key];
        if (key.startsWith('VUE_')) {
          res[key] = element;
        }
      }
    }
    return res;
  }
}