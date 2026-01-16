///<reference path="TestModule.ts"/>
///<reference path="../com/hydrotik/geom/Point.ts"/>


module app {

    import TestModule = app.TestModule;
    import Point = com.hydrotik.geom.Point;

    export class App {

        private view: HTMLElement;

        public model : Model;


        /**
        *   Application Facade
        *   
        *   @param {HTMLElement} view target container
        */
        constructor(view: HTMLElement) {
            this.view = view;

            this.log('Initializing Application from ' + this.view);
        }

        private log(message: string): void {
            console.log(message);
        }

        initializeApplication(message:string): void {
            this.log(message);

            var tm:TestModule = new TestModule('my URL');

            var pt:Point = new Point(25, 75);

            console.log(pt.toString());


        }
    }
}