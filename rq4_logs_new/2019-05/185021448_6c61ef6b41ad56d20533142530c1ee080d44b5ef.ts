import { readFileSync } from "fs";
import path from "path";

namespace Config {
  export interface Token {
    bot: string;
    user: string;
  }

  export interface ChannelSetting {
    name: string;
    maxMessageNumber?: number;
    message?: MessageSetting;
  }

  export interface DefaultSetting {
    maxMessageNumber: number;
    message?: MessageSetting;
  }

  export interface MessageSetting {
    start?: string[];
    running?: string[];
    end?: string[];
  }

  export interface IConfig {
    token: Token;
    channels: ChannelSetting[];
    default: DefaultSetting;
  }

  export function load(): IConfig {
    const configFilePath = path.join(process.cwd(), "config/default.json");
    try {
      const config = readFileSync(configFilePath, "utf-8");
      return JSON.parse(config);
    } catch (e) {
      throw new Error(`Config file ${configFilePath} cannot be read`);
    }
  }
}

export default Config;