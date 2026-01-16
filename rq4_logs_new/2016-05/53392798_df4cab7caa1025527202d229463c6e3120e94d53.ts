import {Application} from "../Application";
/**
 * Defines the contract interface for the configuration manager
 */
export interface ServiceProviderContract {
    /**
     * Called when booting the framework.
     * Here we must register every object to the dependency container
     * We must never use external dependencies because we can't be sure they are registered at this point (use start instead)
     * @param container
     */
    boot(container:Huject.Container);

    /**
     * Called when starting the application
     * Here we must start every thing the service need to work
     * We can use external dependencies because we are sure they are all registered to the container
     * @param app
     */
    start(app:Application, container:Huject.Container);
}
