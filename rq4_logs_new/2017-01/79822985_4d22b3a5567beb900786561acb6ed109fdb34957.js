export var APP_STORAGE_KEY = 'app'

const state = (window.localStorage.getItem(APP_STORAGE_KEY) !== null) ? JSON.parse(window.localStorage.getItem(APP_STORAGE_KEY)) : {
    isInstalled: false,
    device: {
        isMobile: false,
        isTablet: false
    },
    sidebar: {
        opened: false,
        hidden: false
    }
}

const types = {
    TOGGLE_SIDEBAR: 'TOGGLE_SIDEBAR',
    SET_INSTALLED: 'SET_INSTALLED',
    RESET_APP: 'RESET_APP'
}

const mutations = {
    [types.TOGGLE_SIDEBAR] (state, payload) {
        state.sidebar.opened = payload

        // if (!state.isInstalled) {
        //     state.sidebar.opened = false
        //     return
        // }
        // if (state.device.isMobile) {
        //     state.sidebar.opened = payload
        //     return
        // }
        // state.sidebar.opened = true
    },
    [types.SET_INSTALLED] (state, payload) {
        state.isInstalled = payload
    },
    [types.RESET_APP] (state) {
        state.isInstalled = false
        state.device.isMobile = false
        state.device.isTablet = false
        state.sidebar.opened = false
        state.sidebar.hidden = false
    }

}

const actions = {
    toggleSidebar: ({commit}, payload) => commit(types.TOGGLE_SIDEBAR, payload),
    setInstalled: ({commit, dispatch}, payload) => {
        commit(types.SET_INSTALLED, payload)
        if (payload === false) {
            commit(types.RESET_APP)
        }
    }
}

const getters = {
    sidebar: state => state.sidebar,
    isInstalled: state => state.isInstalled
}

export default {state, mutations, actions, getters, types}