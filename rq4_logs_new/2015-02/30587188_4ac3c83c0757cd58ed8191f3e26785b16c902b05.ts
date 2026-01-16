module GameFlow {
    /**
     * Interface for storing and retrieving application state between event loops.
     *
     * The state is fetched via a claim, state can then be manipulated and returned back to the board via swap, which
     * will compare the new state against the old and return a boolean indicating if a change has occurred.
     * Then any side-effects can be invoked with the new state if it has changed, and finally a release.
     *
     * Between a claim and release another claim cannot be made - a error is thrown if so.
     */
    export interface Board<S> {
        /**
         * Claim the latest state
         * @return the state
         * @throws if another claim is attempt before a release
         */
        claim(): S;

        /**
         * Store the state
         * @param state
         * @return true if the state has changed
         * @throws if the state has not been claimed
         */
        swap(state:S): boolean;

        /**
         * Release the claim on the state
         * @throws if the state has not been claimed
         */
        release(): void;
    }

    /**
     * A Player is a function that takes a cue and state, and returns a new state.
     * It MUST be a pure, referentially transparent function. ie. uses only its arguments, takes no data from outside,
     * and has no side-effects. It must be entirely synchronous.
     */
    export interface Player<C,S> {
        (cue:C, state:S): S;
    }

    /**
     * An Spectator takes the final state of a Round and executes any necessary side-effects.
     * eg. rendering to DOM, performing HTTP requests, or any other asynchronous action
     * It should never directly trigger a new Round.
     */
    export interface Spectator<S> {
        (state:S): void;
    }

    /**
     * The interface for triggering the next Round, given a cue.
     * This method will apply all Players to the state, and then execute all Spectators if the state has changed.
     */
    export interface Round<C> {
        (cue:C): void;
    }

    /**
     * Create a Board, given an initial State
     */
    export function board<S>(initialState:S):Board<S> {
        var state = initialState;
        var claimed = false;

        return {
            claim() {
                if (claimed) {
                    throw "Attempted to claim a state twice";
                }
                claimed = true;
                return state;
            },

            swap(newState) {
                if (!claimed) {
                    throw "Attempted to swap an unclaimed state";
                }
                if (newState !== state) {
                    state = newState;
                    return true;
                }
                return false;
            },

            release() {
                if (!claimed) {
                    throw "Attempted to release an unclaimed state";
                }
                claimed = false;
            }
        };
    }

    /**
     * Create a Round given a Board (holding a State), Player and Spectator
     * This round can then be used as an Event handler function
     *
     * The args are ordered so that this fn could be partially applied with a common Board and delegating Spectator,
     * then all that is needed is the Player(s) for each specific Event.
     */
    export function round<C,S>(board:Board<S>, spectator:Spectator<S>, player:Player<C,S>):Round<C> {
        return function (cue:C):void {

            // Get starting state
            var startState = board.claim();

            try {
                // Call the player with the state from the board
                var endState = player(cue, startState);

                // Store state and determine if a change has occurred
                if (board.swap(endState)) {

                    // Call spectator only if state has changed
                    spectator(endState);
                }
            } finally {
                board.release();
            }
        };
    }

// For more complex configurations that require multiple Players and/or Spectators...

    /**
     * Interface for the collection of Players - that simply implements reduce.
     * Example implementations: Array, Immutable.Iterable (from immutable-js)
     */
    export interface PlayerCollection<C,S> {
        reduce(reducer:(state:S, player:Player<C,S>) => S, initialState:S): S;
    }

    /**
     * Interface for the collection of Spectators - that simply implements forEach.
     * Example implementations: Array, Immutable.Iterable (from immutable-js)
     */
    export interface SpectatorCollection<S> {
        forEach(sideEffect:(spectator:Spectator<S>) => any): any;
    }


    /**
     * Create a Player that chains a collection of Players
     */
    export function players<C,S>(players:PlayerCollection<C,S>):Player<C,S> {
        return function (cue:C, state:S):S {
            // Call each player with the cue and state from the previous player
            return players.reduce((state, player) => player(cue, state), state);
        };
    }

    /**
     * Create a delegating Spectator for a collection of Spectators
     */
    export function spectators<S>(spectators:SpectatorCollection<S>):Spectator<S> {
        return function (state:S):void {
            // Call each spectators with the state
            spectators.forEach(spectator => {
                spectator(state);
            });
        };
    }
}