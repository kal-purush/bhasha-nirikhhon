
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


var elixir = require('laravel-elixir');

elixir.config.registerWatcher("default", "resources/assets/**");

var bowerDir = './resources/assets/vendor/';
 
var lessPaths = [
    bowerDir + "bootstrap/less",
    bowerDir + "font-awesome/less",
    bowerDir + "bootstrap-select/less",
    bowerDir + "seiyria-bootstrap-slider/less",
    bowerDir + "classy-bootstrap/less"
];

elixir(function(mix) {
    mix.less(
            'app.less',  //src - assumes resources/assets/less
            'public/css/tpp.css', //output - defaults to 
            {
                paths: lessPaths
            }
        ).scripts(
            [
               'jquery/dist/jquery.min.js',
                'bootstrap/dist/js/bootstrap.min.js',
                'bootstrap-select/dist/js/bootstrap-select.min.js',
                'handlebars/handlebars.min.js',
                'moment/moment.js',
                'jquery.easing/js/jquery.easing.js',
                'seiyria-bootstrap-slider/js/bootstrap-slider.js'
            ],
            'public/js/tpp.js',
            bowerDir)
        .copy('resources/assets/js/app.js', 'public/js/app.js')
        .copy('resources/assets/img', 'public/img')
        .copy(bowerDir + 'font-awesome/fonts', 'public/fonts');

});