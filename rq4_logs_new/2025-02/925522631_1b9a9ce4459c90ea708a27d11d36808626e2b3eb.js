import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'

const TRICK_MENU_ITEMS = [
    'SHAKE',
    'SPIN',
    'ROLLOVER',
    'STAND',
    'STAY',
    'COME'
]

const GAME_MENU_ITEMS = [
    'ROCK',
    'PAPER',
    'SCISSORS'
]

export default create(subscribeWithSelector((set) =>
{
    return {
        mode: 'TRICK',
        menuItems: TRICK_MENU_ITEMS,
        menuItem: TRICK_MENU_ITEMS[0],
        trick: 'NONE',

        toggleMode: () =>
        {
            set((state) =>
            {
                if(state.mode === 'TRICK') return { mode: 'GAME', menuItems: GAME_MENU_ITEMS, menuItem: GAME_MENU_ITEMS[0] }
                if(state.mode === 'GAME') return { mode: 'TRICK', menuItems: TRICK_MENU_ITEMS, menuItem: TRICK_MENU_ITEMS[0] }
            })
        },
        selectMenuItem: () => {
            set((state) =>
            {
                if (state.mode === 'TRICK') return { trick: state.menuItem }
                return {}
            })
        },
        unselectMenuItem: () => {
            set((state) =>
            {
                if (state.mode === 'TRICK') return { trick: 'NONE' }
                return {}
            })
        },
        nextMenuItem: () => {
            set((state) =>
            {
                let index = state.menuItems.indexOf(state.menuItem);
                const nextIndex = (index + 1) % state.menuItems.length
                return { menuItem: state.menuItems[nextIndex]};
            })
        },
        previousMenuItem: () => {
            set((state) =>
            {
                let index = state.menuItems.indexOf(state.menuItem);
                const previousIndex = (index === 0) ? state.menuItems.length - 1 : (index - 1)
                return { menuItem: state.menuItems[previousIndex]};
            })
        }

    }
}))