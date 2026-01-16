/// <reference path="typings/require/require.d.ts" />

require.config({
    baseUrl: "js",
    paths: {
        "views": 'js/views',
        "models": 'js/models',
        "collections": 'js/collections'
    }
});

require(['app']);