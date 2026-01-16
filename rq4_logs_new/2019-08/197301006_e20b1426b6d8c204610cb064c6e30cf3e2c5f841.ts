import test from 'ava';
import Metalsmith from 'metalsmith';
import path from 'path';

import netlifyPublishedDate from '../../src';
import { normalizeOptions } from '../../src/options';

test.before(() => {
    process.chdir(path.join(__dirname, 'fixtures'));
});

test('should to import plugin files by plain object', t => {
    const cwd = process.cwd();
    const options = normalizeOptions(
        {
            plugins: {
                './plugin-01': true,
                './plugin-02': true,
            },
        },
        netlifyPublishedDate.defaultOptions,
    );

    t.deepEqual(
        options.plugins,
        ['./plugin-01', './plugin-02']
            .map(pluginPath => require(path.join(cwd, pluginPath)))
            .map(pluginGenerator => pluginGenerator(true)),
    );
});

test('should pass the options value to the plugin generator function specified in plain object', async t => {
    const optionValue = { x: 42 };
    const options = normalizeOptions(
        {
            plugins: {
                './echo-opts-plugin': optionValue,
            },
        },
        netlifyPublishedDate.defaultOptions,
    );
    const files = await new Promise<Metalsmith.Files>((resolve, reject) => {
        Metalsmith(__dirname)
            .use([...options.plugins])
            .run({}, (err, files) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(files);
                }
            });
    });

    t.is(files.options, optionValue);
});

test('should to import plugin files by array', t => {
    const cwd = process.cwd();
    const options = normalizeOptions(
        {
            plugins: [
                { './plugin-01': true },
                { './plugin-02': true },
                { './plugin-01': true },
                { './plugin-01': true },
            ],
        },
        netlifyPublishedDate.defaultOptions,
    );

    t.deepEqual(
        options.plugins,
        ['./plugin-01', './plugin-02', './plugin-01', './plugin-01']
            .map(pluginPath => require(path.join(cwd, pluginPath)))
            .map(pluginGenerator => pluginGenerator(true)),
    );
});

test('should pass the options value to the plugin generator function specified in array', async t => {
    const optionValue = { y: 42 };
    const options = normalizeOptions(
        {
            plugins: [{ './echo-opts-plugin': optionValue }],
        },
        netlifyPublishedDate.defaultOptions,
    );

    const files = await new Promise<Metalsmith.Files>((resolve, reject) => {
        Metalsmith(__dirname)
            .use([...options.plugins])
            .run({}, (err, files) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(files);
                }
            });
    });

    t.is(files.options, optionValue);
});

test('import of non-existent script file should fail', t => {
    t.throws(
        () => {
            normalizeOptions(
                {
                    plugins: {
                        './not-found-plugin': true,
                    },
                },
                netlifyPublishedDate.defaultOptions,
            );
        },
        {
            instanceOf: TypeError,
            message: /[Ff]ailed to import plugin "\.\/not-found-plugin" .* option "plugins"/,
        },
    );
});

test('import of script files that do not export functions should fail', t => {
    t.throws(
        () => {
            normalizeOptions(
                {
                    plugins: {
                        './no-func': true,
                    },
                },
                netlifyPublishedDate.defaultOptions,
            );
        },
        {
            instanceOf: TypeError,
            message: /[Pp]lugin "\.\.\/no-func" .* option "plugins" .* not export the function/,
        },
    );
});

test('import of script files exporting functions that do not return function should fail', t => {
    t.throws(
        () => {
            normalizeOptions(
                {
                    plugins: {
                        './invalid-func': true,
                    },
                },
                netlifyPublishedDate.defaultOptions,
            );
        },
        {
            instanceOf: TypeError,
            message: /[Pp]lugin "\.\.\/invalid-func" .* option "plugins" .* not return the function/,
        },
    );
});

test('If the plugin name does not start with "." and "/", should to import it like require() function', t => {
    t.throws(
        () => {
            normalizeOptions(
                {
                    plugins: {
                        '@sounisi5011/example': true,
                    },
                },
                netlifyPublishedDate.defaultOptions,
            );
        },
        {
            instanceOf: TypeError,
            message: /[Ff]ailed to import plugin "@sounisi5011\/example" .* option "plugins"/,
        },
    );
});