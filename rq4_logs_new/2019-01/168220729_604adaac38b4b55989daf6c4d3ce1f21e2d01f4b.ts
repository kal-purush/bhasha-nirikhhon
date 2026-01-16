import { Command, flags } from "@oclif/command";
import * as path from "path";
import Client from "../../lib/client";

export default class IndexDelete extends Command {
  static description = "Delete specified index";

  static flags = {
    index: flags.string({
      char: "i",
      required: true,
      description: "Elasticsearch index"
    })
  };

  async run() {
    const { flags } = this.parse(IndexDelete);
    const config = require(path.join(this.config.configDir, "config.json"));

    const client = await new Client(config.url).client();

    if (!(await client.indices.exists({ index: flags.index }))) {
      this.log(`Specified index ${flags.index} doesn't exist in this cluster`);
      this.exit(1);
    }

    try {
      const response = await client.indices.delete({ index: flags.index });
      if (response.acknowledged) {
        console.log(`Index ${flags.index} has been deleted successfully`);
      }
    } catch (error) {
      this.log(error.message);
      this.exit(1);
    }
  }
}