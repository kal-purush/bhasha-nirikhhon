// Install the angularjs.TypeScript.DefinitelyTyped NuGet package
module App {
    "use strict";

    interface IsampleController {
        birthDate: Date;
        daysAlive: number;
        activate: () => void;
    }

    class sampleController implements IsampleController {
        birthDate: Date;
        daysAlive: number=0;

        static $inject: string[] = ["basicService"];

        constructor(private basicService: IbasicService) {
            this.activate();
        }

        activate() {

        }

        calculate(): void {
            if (this.birthDate == undefined)
                return;
            else {
                this.daysAlive = this.basicService.calculateDays(this.birthDate);
            }
        }

        getData(): void {
            this.basicService.getData();
        }
    }

    angular.module(App.applicationName).controller("sampleController", ['basicService', sampleController]);
}