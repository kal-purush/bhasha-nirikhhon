import { Locator, Page } from "@playwright/test";

export default class Helper{

    constructor(private page: Page) { }
    
    async fluentWait(locator: Locator, condition: () => Promise<boolean>, timeout: number = 5000, retryInterval: number = 100): Promise<void> {
        const startTime = Date.now();
        while (Date.now() - startTime < timeout) {
          if (await condition()) {
            return; // Condition met, exit the function
          }
          await new Promise((resolve) => setTimeout(resolve, retryInterval)); // Wait before retrying
        }
        throw new Error('Condition not met within timeout');
      }
}
