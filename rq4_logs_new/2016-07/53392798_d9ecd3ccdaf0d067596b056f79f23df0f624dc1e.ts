/**
 * Defines interface for a custom loader
 */
export interface LoaderContract {
    loadModule(name: string, module: any): void;
}