import { Logger } from "@mohism/utils";
import { parseExpression } from "cron-parser";
import { EOL } from "os";
import { ICronexpr } from "./type";

const logger = Logger();

export default class CronRunner {
  private jobs: Array<ICronexpr>;
  constructor(jobs: Array<ICronexpr>) {
    this.jobs = jobs;
  }

  exec() {
    this.jobs.forEach(job => {
      this.run(job);
    });
  }

  private async run({ expr, func, name }: ICronexpr) {
    try {
      const next = parseExpression(expr).next().getTime() - Date.now();
      setTimeout(async () => {
        logger.info(`Run [${name}] at ${new Date().toLocaleTimeString()}`);
        await func();
        logger.info(`Run [${name}] Done!${EOL}`);
        this.run({ expr, func, name });
      }, next);
    } catch (e) {
      logger.err(`[${name}] ${e.message}`);
    }
  }
}