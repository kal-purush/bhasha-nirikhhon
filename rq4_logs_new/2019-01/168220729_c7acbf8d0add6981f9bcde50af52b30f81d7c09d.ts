import { Command, flags } from "@oclif/command";
import Ingester from "../lib/ingester";
import * as path from "path";
import * as fs from "fs";
import * as Progress from "ascii-progress";

import EsClient from "../lib/client";

export default class Ingest extends Command {
  static description = "Ingest data into the cluster";

  static flags = {
    index: flags.string({
      char: "i",
      description: "Elasticsearch Index",
      required: true,
      dependsOn: ["type", "src"]
    }),
    type: flags.string({
      char: "t",
      description: "Elasticsearch Type",
      required: true,
      dependsOn: ["index", "src"]
    }),
    src: flags.string({
      char: "s",
      description: "Location of data json file",
      required: true,
      dependsOn: ["type", "index"]
    })
  };

  loadData(src: string) {
    if (!fs.existsSync(src)) {
      console.log("Specified file doesn't exist");
      process.exit();
    }

    return require(path.resolve(src));
  }

  async run() {
    const { flags } = this.parse(Ingest);
    const { index, type, src } = flags;
    const data = this.loadData(src);
    const client = new EsClient().client;

    const ingester = new Ingester({
      client,
      index,
      type,
      data
    });

    const progress = new Progress({
      schema: "[:bar.white] :current/:total :percent",
      total: data.length
    });

    ingester.start().subscribe({
      next: _ => progress.tick(),
      complete: _ => {
        progress.clear();
        console.log(`${data.length} documents have been successfuly indexed`);
      },
      error: (error: Error) => console.log(error.message)
    });
  }
}