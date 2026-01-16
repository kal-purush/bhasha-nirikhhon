/// <reference path="Component.ts" />

class Application extends Component {
    public static instance;

    constructor(parent?: HTMLElement) {
        Application.instance = this;
        super("Application");
        this.title.value = "Application Demo";
    }

    public start(): void {

    }
}

window.addEventListener("load",() => {
    new Application().show();
});