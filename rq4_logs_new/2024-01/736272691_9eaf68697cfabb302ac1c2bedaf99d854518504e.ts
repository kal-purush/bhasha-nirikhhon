export class SwitchContextUtil {
    async switchContext(contextName: string, timeout?: number): Promise<void> {
        const actualTimeout = timeout ?? 60000;

        try {
            const currentContext = await driver.getContext();
            if (currentContext.toString().includes(contextName)) {
                console.log(`Already in ${contextName} View.`);
                return;
            }

            await driver.waitUntil(
                async () => (await driver.getContexts()).some(context => context.toString().includes(contextName)),
                { timeout: actualTimeout }
            );

            const contexts = await driver.getContexts();
            const contextsArray: string[] = contexts as string[];
            const context = contextsArray.find(context => context.includes(contextName));

            if (!context) {
                throw new Error(`Context '${contextName}' not found.`);
            }

            await driver.switchContext(context);
        } catch (err) {
            console.error(`Error during switching to ${contextName} view: \n${err.stack}`);
            throw err;
        }
    }   

    async switchToNativeContext(timeout?: number): Promise<void> {
        await this.switchContext('NATIVE_APP', timeout);
    }

    async switchToWebContext(timeout?: number): Promise<void> {
        await this.switchContext('WEBVIEW', timeout);
    }
}
