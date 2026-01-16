import { writable } from "svelte/store";

function createDrawer() {
    const { subscribe, set } = writable(false);

    return {
        subscribe,
        set,
        drawerOpen: () => set(true),
        drawerClose: () => set(false)
    };
}

export const drawer = createDrawer();