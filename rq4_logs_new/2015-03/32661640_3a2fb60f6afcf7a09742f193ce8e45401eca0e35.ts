
module com.hydrotik.elasticlists {

    export class App {

        private view: HTMLElement;
        private perspective: number;
        private surface: ParallaxSurface[];

        /**
        *   Application Facade
        *   
        *   @param {HTMLElement} scrollableContent The container that will be parallaxed
        */
        constructor(view: HTMLElement) {
            this.view = view;

            this.log('Hello App. Show me ' + this.view);
        }

        private log(message: string): void {
            console.log(message);
        }

        initializeApplication(message:string): void {
            this.log(message);
        }
    }
}