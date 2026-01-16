import { Command } from "cliffy/command.ts";
import { VERSION, DENO_IMPORT_MAP } from "./constants.ts";
import JsonParser from "./jsonParser.ts";
import ScriptRunner from "./scriptRunner.ts";

export default class CommandBuilder {
  public constructor(
    private parser: JsonParser = new JsonParser(),
    private scriptRunner: ScriptRunner = new ScriptRunner(parser),
  ) {
  }

  public buildCommand(): Command {
    const cmd = new Command()
      .allowEmpty()
      .version(VERSION)
      .description("Manage you project with package.json file.");
    this.buildInstallCommand(cmd);
    this.buildDefaultCommands(cmd);
    this.buildCommandFromPackageJson(cmd);
    return cmd;
  }

  private buildInstallCommand(cmd: Command): void {
    cmd.command(
      "install",
      new Command()
        .arguments("[...names@version:string]")
        .example("Without version", "ddm install http")
        .example("With version", "ddm install http@v0.50.0")
        .example("Multiple package", "ddm install http log fmt")
        .description("Install one or more package")
        .useRawArgs()
        .action((flags, ...args) => {
          this.scriptRunner.runScript("install", ...args);
        }),
    ).default("install");
  }

  private buildDefaultCommands(cmd: Command): void {
    DENO_IMPORT_MAP.forEach((denoCmd) => {
      cmd.command(
        denoCmd,
        new Command()
          .arguments("[...parameters:string]")
          .description(`Run 'deno ${denoCmd}' command`)
          .useRawArgs()
          .action((flags, ...args) => {
            this.scriptRunner.runScript(denoCmd, ...args);
          }),
      );
    });
  }

  private buildCommandFromPackageJson(cmd: Command): void {
    const json = this.parser.readPackageJson();
    Object.keys(json?.scripts).forEach((script) => {
      cmd.command(
        script,
        new Command()
          .arguments("[...parameters:string]")
          .description(`Run ${script} script defined in package.json`)
          .useRawArgs()
          .action((flags, ...args) => {
            this.scriptRunner.runScript(script, ...args);
          }),
        true,
      );
    });
  }
}