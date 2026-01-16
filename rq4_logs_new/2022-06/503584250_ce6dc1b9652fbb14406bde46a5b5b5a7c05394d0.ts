import { Command } from "@oclif/core";
import { turnOff } from "@ryanwersal/litra";

export default class OffCommand extends Command {
  static description = "Turn off Litra";

  async run() {
    await this.parse(OffCommand);
    turnOff();
  }
}