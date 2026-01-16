import {Subscription} from "rxjs/Subscription";

/**
 * Defines an interface that represents a handler function for an interaction.
 * These functions return a value (either null or a value of type TOutput) upon handling the
 * interaction, or undefined if the handler is not able to handle the interaction. 
 */
export interface IReactiveInteractionHandler<TInput, TOutput> {
    (param: TInput): TOutput | Promise<TOutput>;
}

/**
 * Defines a class that represents an interaction. 
 * Interactions are designed to provide a means of resolving view-specific input mechanisms that occur
 * during an operation that a ReactiveObject is performing. 
 */
export class ReactiveInteraction<TInput, TOutput> {
    private _handlerChain: IReactiveInteractionHandler<TInput, TOutput>[] = [];

    /**
     * Adds the given handler to the beginning of the handler chain for this interaction and returns
     * a subscription that, when unsubscribed from, removes the handler from the handler chain.
     * @param handler The function that can handle the interaction.
     */
    public registerHandler(handler: IReactiveInteractionHandler<TInput, TOutput>): Subscription {
        if (!handler) throw Error("Null or undefined handlers cannot be registered. Pass in a valid handler function to properly register it.");
        this._handlerChain.unshift(handler);
        return new Subscription(() => {
            var index = this._handlerChain.indexOf(handler);
            if (index >= 0) {
                this._handlerChain.splice(index, 1);
            }
        });
    }

    /**
     * Attempts to handle an interaction. Returns a promise that represents the async operation.
     * By default, handlers are called from most recently registered to least recently registered.
     * If a handler returns (note that if the handler resolves a promise with undefined, that will be used as the output) undefined, then the next handler in line is called.
     * If a handler errors, then the entire chain errors and the error is surfaced through the returned promise.
     * @param param The input that should be provided to the handler.
     */
    public handle(param?: TInput): Promise<TOutput> {
        return new Promise<TOutput>((resolve, reject) => {
            var currentHandler = 0;
            do {
                var handler = this._handlerChain[currentHandler];
                try {
                    var result = handler(param);
                    if (typeof result !== "undefined") {
                        if (result instanceof Promise) {
                            (<Promise<TOutput>>result).then(value => {
                                resolve(value);
                            }, err => {
                                reject(err);
                            });
                        } else {
                            resolve(result);
                        }
                        return;
                    }
                } catch (err) {
                    reject(err);
                }
            } while (++currentHandler < this._handlerChain.length);
            reject(new Error("No handler handled the interaction. Make sure that registerHandler is being called and that its returned subscription is not being disposed."));
        });
    }
}