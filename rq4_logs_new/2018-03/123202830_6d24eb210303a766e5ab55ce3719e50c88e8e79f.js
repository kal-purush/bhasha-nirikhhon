var elixir = require('laravel-elixir');

require('laravel-elixir-images');

/*
 |--------------------------------------------------------------------------
 | Elixir Asset Management
 |--------------------------------------------------------------------------
 |
 | Elixir provides a clean, fluent API for defining some basic Gulp tasks
 | for your Laravel application. By default, we are compiling the Less
 | file for our application, as well as publishing vendor resources.
 |
 */

elixir(function(mix) {
    mix
        .copy("resources/assets/fonts", "public/build/fonts")
        .copy("resources/assets/images/*.ico", "public/images")
        .copy("resources/assets/images/*.gif", "public/images")
        .images([
            "*.png",
            "*.jpg"
        ], "public/images", {
            webp: false,
            sizes: [[]],  // Only generate original image size
            optimize: true
        })
        .styles([
            "app.css",
            "./node_modules/bootstrap-switch/dist/css/bootstrap3/bootstrap-switch.min.css"
        ], "public/css/vendor.css")
        .styles([
            "custom.css"
        ], "public/css/custom.css")
        .scripts([
            "./node_modules/bootstrap-switch/dist/js/bootstrap-switch.min.js"
        ], "public/js/vendor.js")
        .scripts([
            "./node_modules/jquery/dist/jquery.min.js"
        ], "public/js/jquery.min.js")
        .version([
            "images",
            "css",
            "js"
        ]);
});