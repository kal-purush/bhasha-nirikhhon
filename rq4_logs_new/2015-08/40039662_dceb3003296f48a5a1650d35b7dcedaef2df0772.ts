/**
 * Created by airead on 15/8/1.
 */
module game {
    export class ApplicationFacade extends puremvc.Facade implements puremvc.IFacade {
        public  constructor() {
            super();
        }

        public static STARTUP: string = "startup";

        public static getInstance():ApplicationFacade {
            if (this.instance == null) {
                this.instance = new ApplicationFacade();
            }
            return <ApplicationFacade>this.instance;
        }

        public initializeController():void {
            super.initializeController();

            this.registerCommand(ApplicationFacade.STARTUP, controller.StartupCommand);
        }

        public startUp(rootView: egret.DisplayObjectContainer):void {
            this.sendNotification(ApplicationFacade.STARTUP, rootView);
            this.removeCommand(ApplicationFacade.STARTUP);
        }
    }

}