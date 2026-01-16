import test from 'ava';
import Metalsmith from 'metalsmith';
import path from 'path';

import netlifyPublishedDate from '../../src';
import { normalizeOptions } from '../../src/options';
import { OptionsInterface } from '../../src/plugin';

const metadata = {
    files: {},
    filename: '',
    fileData: { contents: Buffer.from([]) },
    metalsmith: Metalsmith(__dirname),
};

test.before(() => {
    process.chdir(path.join(__dirname, 'fixtures'));
});

test('should pass the function to the options value', t => {
    const func: OptionsInterface['contentsConverter'] = (
        contents: Buffer,
    ): Buffer => contents;
    const options = normalizeOptions(
        {
            contentsConverter: func,
        },
        netlifyPublishedDate.defaultOptions,
    );

    t.is(options.contentsConverter, func);
});

test('should import external script file', async t => {
    const options = normalizeOptions(
        {
            contentsConverter: 'mod.ret-buffer.js',
        },
        netlifyPublishedDate.defaultOptions,
    );

    t.deepEqual(
        await options.contentsConverter(Buffer.from(''), metadata),
        Buffer.from('42'),
    );
});

test('should import external script file without .js extension', async t => {
    const options = normalizeOptions(
        {
            contentsConverter: 'mod.ret-buffer',
        },
        netlifyPublishedDate.defaultOptions,
    );

    t.deepEqual(
        await options.contentsConverter(Buffer.from(''), metadata),
        Buffer.from('42'),
    );
});

test('should import external script file that returns a promise', async t => {
    const options = normalizeOptions(
        {
            contentsConverter: 'async-mod.ret-buffer',
        },
        netlifyPublishedDate.defaultOptions,
    );

    t.deepEqual(
        await options.contentsConverter(Buffer.from(''), metadata),
        Buffer.from('42'),
    );
});

test('import of script files exporting functions that do not return Buffer should fail', t => {
    t.throwsAsync(
        async () => {
            const options = normalizeOptions(
                {
                    contentsConverter: './invalid-func',
                },
                netlifyPublishedDate.defaultOptions,
            );

            await options.contentsConverter(Buffer.from(''), metadata);
        },
        {
            instanceOf: TypeError,
            message: /[Mm]odule "\.\/invalid-func" .* option "contentsConverter" .* not return a Buffer/,
        },
    );
});

test('import of script files exporting functions that do not return Promise<Buffer> should fail', t => {
    t.throwsAsync(
        async () => {
            const options = normalizeOptions(
                {
                    contentsConverter: './async-mod.ret-number',
                },
                netlifyPublishedDate.defaultOptions,
            );

            await options.contentsConverter(Buffer.from(''), metadata);
        },
        {
            instanceOf: TypeError,
            message: /[Mm]odule "\.\/async-mod.ret-number" .* option "contentsConverter" .* not return a Buffer/,
        },
    );
});

test('import of script files that do not export functions should fail', t => {
    t.throws(
        () => {
            normalizeOptions(
                {
                    contentsConverter: './no-func',
                },
                netlifyPublishedDate.defaultOptions,
            );
        },
        {
            instanceOf: TypeError,
            message: /[Mm]odule "\.\/no-func" .* option "contentsConverter" .* not export the function/,
        },
    );
});

test('import of non-existent script file should fail', t => {
    t.throws(
        () => {
            normalizeOptions(
                {
                    contentsConverter: './not-found-mod',
                },
                netlifyPublishedDate.defaultOptions,
            );
        },
        {
            instanceOf: TypeError,
            message: /[Ff]ailed to import module "\.\/not-found-mod" .* option "contentsConverter"/,
        },
    );
});

test('If the module name does not start with "." and "/", should to import it like require() function', t => {
    t.throws(
        () => {
            normalizeOptions(
                {
                    contentsConverter: '@sounisi5011/example',
                },
                netlifyPublishedDate.defaultOptions,
            );
        },
        {
            instanceOf: TypeError,
            message: /[Ff]ailed to import module "@sounisi5011\/example" .* option "contentsConverter"/,
        },
    );
});