import {Module, ModuleInitOptions} from "microframework/Module";

/**
 * T-Validator module integration with microframework.
 */
export class TValidatorModule implements Module {

    // -------------------------------------------------------------------------
    // Properties
    // -------------------------------------------------------------------------

    private options: ModuleInitOptions;

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    getName(): string {
        return 'TValidatorModule';
    }

    init(options: ModuleInitOptions): void {
        this.options = options;
    }

    onBootstrap(dependentModules?: Module[]): Promise<any> {
        return Promise.resolve();
    }

    onShutdown(): Promise<any> {
        return Promise.resolve();
    }

}