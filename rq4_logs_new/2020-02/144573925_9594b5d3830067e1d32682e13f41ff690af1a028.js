module.exports = {
    moduleFileExtensions: [
        "js",
        "vue"
    ],
    preset: "@vue/cli-plugin-unit-jest",
    testEnvironment: 'jsdom',
    transform: {
        ".*\\.(vue)$": "vue-jest"
    },
};