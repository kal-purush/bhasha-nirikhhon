import {ServiceProvider} from "./ServiceProvider";
import {AutoLoader} from "./AutoLoader";
import {Configuration} from "./Configuration";
import FactoryMethod = Huject.FactoryMethod;
import {Application} from "./Application";
/**
 * Defines the base service provider
 *
 * Basically register all TSFW related classes
 */
export class TSFWServiceProvider extends ServiceProvider {

    boot(container:Huject.Container) {
        //Register default framework classes
        container.register("AutoLoader", AutoLoader).as(FactoryMethod.SINGLETON);
        container.register("Configuration", Configuration).as(FactoryMethod.SINGLETON);
    }

    start(app:Application, container:Huject.Container) {
        //Start any component that have to be started
    }

}