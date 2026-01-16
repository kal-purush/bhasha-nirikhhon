import { Command, flags } from "@oclif/command";
import * as fs from "fs-extra";
import * as path from "path";
import { catchError } from "rxjs/operators";

export default class Config extends Command {
  static description = "describe the command here";

  static flags = {
    uri: flags.help({
      char: "h",
      description: "Elasticsearch Host URI",
      required: true
    })
  };

  async run() {
    const { flags } = this.parse(Config);

    if (!fs.existsSync(this.config.configDir)) {
      fs.mkdirSync(this.config.configDir);
    }

    const file = path.join(this.config.configDir, "config.json");

    try {
      await fs.writeFile(file, JSON.stringify({ url: flags.uri }));
    } catch (error) {
      this.log(error.message);
      process.exit();
    }

    const config = await fs.readJSON(file);

    this.log(`Elasticsearch host has been set to ${config.url}`);
  }
}