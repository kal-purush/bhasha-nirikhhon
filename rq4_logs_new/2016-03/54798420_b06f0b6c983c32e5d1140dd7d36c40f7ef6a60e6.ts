import {IMatchMediaChannelFactory} from "../contracts/IMatchMediaChannelFactory";
import {IMatchMediaChannelConfiguration} from "../contracts/IMatchMediaChannelConfiguration";
import {IMatchMediaChannel} from "../contracts/IMatchMediaChannel";

export default class MatchMediaChannelFactory<TObserver> implements IMatchMediaChannelFactory<TObserver> {
    private _matchMedia: (mediaQuery: string) => MediaQueryList;

    constructor(window: Window) {
        this._matchMedia = MatchMediaChannelFactory.ensureMatchMedia(window);
    }

    public static ensureMatchMedia(window: Window) {
        if (!window.matchMedia) {
            throw new Error("matchMedia must be defined on window");
        }
        return window.matchMedia;
    }

    public create(config: IMatchMediaChannelConfiguration): IMatchMediaChannel<TObserver> {
        throw new Error("not implemented");
    }
}