requirejs.config({
    paths: {
        text: "../Lib/requirejs/text",
        jquery: "../Lib/jquery/jquery.min",
        knockout: "../Lib/knockout/knockout.min",
        bootstrap: "../Lib/bootstrap/js/bootstrap.min",
    },
    shim: {
        bootstrap: {
            deps: ['jquery']
        }
    },
    waitSeconds: 20,
    urlArgs: "bust=" + CacheBuster
});
require(['NameConventionLoader', 'knockout', 'bootstrap'], function (nameConventionLoader, knockout) {
    knockout.components.loaders.push(new nameConventionLoader("TierList"));
    knockout.applyBindings();
});