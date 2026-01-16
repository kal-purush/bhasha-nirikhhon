import {FactoryMethod} from 'huject';
import {ServiceProvider} from "../Core/ServiceProvider";
import {Application} from "../Core/Application";
import {ApplicationContract} from "../Core/Contracts/ApplicationContract";
import {HttpServer} from "../Http/HttpServer";

/**
 * Load all view engines
 */
export class ViewServiceProvider extends ServiceProvider {

    boot(container:Huject.Container) {
    }

    start(app:Application, container:Huject.Container) {
        //Start any component that have to be started
        var application: ApplicationContract = container.resolve("Application");
        var httpServer: HttpServer = container.resolve("HttpServer");
        httpServer.getExpress().set('views',application.getRootDirectory()+'/resources/views');
    }

}