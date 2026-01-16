/// <reference path="../typings/tsd.d.ts"/>

export class MyService {
    constructor() {
    }

    public static addService(a, b) {
        console.log(a);
        return a + b + 10000;
    }
}

export class MyController {
    public message: string;
    public hoge: number;

    constructor(addService) {
        this.init(addService);
    }

    private init(addService) {
        this.message = "Hello world";

        this.hoge = addService(10, 20);
    }

    public action($event) {
        if (this.message.indexOf("o") > 0) {
            this.message = this.message.slice(0, -1);
        } else {
            this.message = "GooooooooooooooodBye!";
        }
        $event.stopPropagation();
        console.log($event);
    }
}