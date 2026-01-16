const IHEIDLoaderFilesystem = require('../lib/twing/loader/filesystem');
const {TwingExtensionDebug} = require('twing');

let loader = new IHEIDLoaderFilesystem('/');

module.exports = {
    componentRoot: 'test',
    componentManifest: 'component.json',
    plugins: {
        jsx: {
            entry: 'index.jsx',
            processors: [
                new (require('../lib/iheid-processor-javascript'))({
                    transform: [
                        ["reactify", {"es6": true}]
                    ]
                })
            ]
        },
        scss: {
            entry: 'index.scss',
            processors: [
                new (require('../lib/iheid-processor-sass'))(Object.assign({}, {
                    precision: 8,
                    sourceMap: true,
                    sourceComments: true,
                    importer: []
                })),
                new (require('../lib/stromboli/stromboli-processor-css-rebase'))()
            ]
        },
        twig: {
            entry: 'index.html.twig',
            processors: [
                new (require('../lib/iheid-processor-twig'))({
                    loader: loader,
                    options: {
                        cache: 'tmp/twig',
                        auto_reload: true,
                        debug: true
                    }
                })
            ]
        }
    }
};