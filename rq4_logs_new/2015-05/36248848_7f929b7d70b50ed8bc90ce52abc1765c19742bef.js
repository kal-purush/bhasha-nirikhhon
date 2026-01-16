Package.describe({
    name: 'cbga:core',
    version: '0.0.1',
    // Brief, one-line summary of the package.
    summary: 'Card and Board Game App API and core functionality',
    // URL to the Git repository containing the source code for this package.
    git: 'https://github.com/lalomartins/cbga-core.git',
    // By default, Meteor will default to using README.md for documentation.
    // To avoid submitting documentation, set this field to null.
    documentation: 'README.md'
});

Package.onUse(function(api) {
    api.versionsFrom('1.1.0.2');
    api.use([
        'coffeescript',
        'random',
        'raix:eventemitter'
    ]);
    api.addFiles([
        'core.coffee',
        'player.coffee',
        'game.coffee',
        'rules.coffee'
    ]);
    api.addFiles([
    ], 'client');
    api.addFiles([
    ], 'server');
    api.addFiles([
    ], 'web.cordova');
    api.export('CBGA');
});