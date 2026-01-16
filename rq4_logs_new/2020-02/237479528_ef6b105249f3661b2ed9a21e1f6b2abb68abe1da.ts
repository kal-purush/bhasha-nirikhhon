import { Termination, terminate } from './common/termination';
import { HelperError } from './docker';
import { InitializedResources } from './resources';

export class Context {
    private readonly __terminables: Termination[];
    private readonly __resources: Readonly<InitializedResources>;
    private __isClosed: boolean;

    constructor(
        terminables: Termination[] = [],
        values: InitializedResources = {},
    ) {
        this.__terminables = terminables;
        this.__resources = Object.freeze(values);
        this.__isClosed = false;
    }

    public get resources(): Readonly<InitializedResources> {
        return this.__resources;
    }

    public async destroy(): Promise<void> {
        if (this.__isClosed) {
            return Promise.reject(new HelperError('Context is already closed'));
        }

        this.__isClosed = true;

        return terminate(...this.__terminables);
    }
}