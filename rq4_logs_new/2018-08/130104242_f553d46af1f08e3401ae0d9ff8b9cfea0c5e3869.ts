/*jshint node: true */
'use strict';

export abstract class SkyA11y {
  private static axeBuilder: any = require('axe-webdriverjs');
  private static axeConfig: any = require('@blackbaud/skyux-builder/config/axe/axe.config');
  private static protractor = require('protractor');
  private static logger = require('@blackbaud/skyux-logger');

  public static run(): Promise<any> {
    return this.protractor.browser.getCurrentUrl().then((url: string) => {
      return new Promise((resolve: any) => {
        const config = this.axeConfig.getConfig();

        this.logger.info(`Starting accessibility checks for ${url}...`);

        this.axeBuilder(this.protractor.browser.driver)
          .options(config)
          .analyze((results: any) => {
            const numViolations = results.violations.length;
            const subject = (numViolations === 1) ? 'violation' : 'violations';

            this.logger.info(`Accessibility checks finished with ${numViolations} ${subject}.\n`);

            if (numViolations > 0) {
              this.logViolations(results);
            }

            resolve(numViolations);
          });
      });
    });
  }

  private static logViolations(results: any): void {
    results.violations.forEach((violation: any) => {
      const wcagTags = violation.tags
        .filter((tag: string) => tag.match(/wcag\d{3}|^best*/gi))
        .join(', ');

      const html = violation.nodes
        .reduce(
          (accumulator: string, node: any) => `${accumulator}\n${node.html}\n`,
          '       Elements:\n'
        );

      const error = [
        `aXe - [Rule: \'${violation.id}\'] ${violation.help} - WCAG: ${wcagTags}`,
        `       Get help at: ${violation.helpUrl}\n`,
        `${html}\n\n`
      ].join('\n');

      this.logger.error(error);
    });
  }
}