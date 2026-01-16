declare var System:any;

// Apply the CLI SystemJS configuration.
System.config({
    map: {
        '@angular': 'node_modules/@angular',
        'rxjs': 'node_modules/rxjs',
        'scripts/main': 'scripts/main.js',
        'ng2-bootstrap': 'node_modules/ng2-bootstrap',
        'moment': 'node_modules/moment/moment.js'
    },
    packages: {
        '@angular/core': {main: 'index'},
        '@angular/common': {main: 'index'},
        '@angular/compiler': {main: 'index'},
        '@angular/platform-browser': {main: 'index'},
        '@angular/platform-browser-dynamic': {main: 'index'},

        // Thirdparty barrels.
        'rxjs': {main: 'Rx'},
        'ng2-bootstrap': {main: 'ng2-bootstrap'},

        // App specific barrels.
        'components': {main: 'index'},
        'services': {main: 'index'},
    }
});