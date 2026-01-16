import { States } from '@apestaartje/finite-state-machine/dist/state/States';

import { Event } from '@tetris/finite-state-machine/Event';
import { State } from '@tetris/finite-state-machine/State';

export const config: States = {
    initial: State.Home,
    states: {
        [State.Home]: {
            on: {
                [Event.Game]: State.Game,
                [Event.Help]: State.Help,
                [Event.HighScore]: State.HighScore,
            },
        },
        [State.Game]: {
            on: {
                [Event.Home]: State.Home,
                [Event.HighScore]: State.HighScore,
            },
        },
        [State.Help]: {
            on: {
                [Event.Home]: State.Home,
                [Event.HighScore]: State.HighScore,
            },
        },
        [State.HighScore]: {
            on: {
                [Event.Home]: State.Home,
            },
        },
    },
};