import {JSWorksLib} from "jsworks/dist/dts/jsworks";
import {AbstractController} from "../AbstractController";


declare const JSWorks: JSWorksLib;


@JSWorks.Controller
export class MultiplayerController extends AbstractController {

    public onCreate(): void {

    }

    public onNavigate(args: object):void {

        super.onNavigate(args);

        const socket: WebSocket = new WebSocket(JSWorks.config['backendSocket']);

        socket.onopen = (event: Event) => {
            alert('соединение открыто!!');
            console.log(event);
        };
    }
}