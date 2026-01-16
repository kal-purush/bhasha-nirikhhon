export default {
    debug: false,
    user: null,
    get(state) {
        return this[state]
    }
}