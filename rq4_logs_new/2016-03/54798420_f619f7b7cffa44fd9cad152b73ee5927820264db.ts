import {Observable} from "rx";

interface INamedMediaQuery {
    name: string;
    mediaQuery: string;
}

export class ObservableMatchMedia {
    private _observers: Set;

    constructor(window: Window, ...mediaQueries: INamedMediaQuery[]) {
        ObservableMatchMedia.ensureMatchMediaIsPresent(window);
        // this._handlerMap = new Map();
        this.bootstrapMediaQueries(window, mediaQueries);
    }

    private bootstrapMediaQueries(window: Window, mediaQueries: INamedMediaQuery[]) {
        for (const named of mediaQueries) {
            const list = window.matchMedia(named.mediaQuery);
            this.registerHandlerFor(named.name, list);
        }
    }

    private registerHandlerFor(identifier: string, list: MediaQueryList) {
        this._observers[identifier] = Observable.fromEventPattern(
            (listener) => list.addListener(listener),
            (listener) => list.removeListener(listener)
        );
    }

    private static ensureMatchMediaIsPresent(window: Window) {
        if (!window.matchMedia) {
            throw new Error("matchMedia must be present");
        }
    }

    public Dispose() {
        console.log("disposing");
    }
}