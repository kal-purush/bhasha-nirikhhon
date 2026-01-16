/// <reference path="/home/pandu/c/dtyped/node/node.d.ts" />
/// <reference path="/home/pandu/c/dtyped/underscore/underscore.d.ts" />
/// <reference path="/home/pandu/c/dtyped/underscore.string/underscore.string.d.ts" />

import fs = require('fs');
import _ = require('underscore');
import _s = require('underscore.string');

var Foo = {}; // TODO better name

export function tmcat(): void {
    var module_name = get_module_name_from_argv('tmcat');
    var filepaths = get_filepaths(module_name);
    var result = _.chain(filepaths)
                  .map(read_file)
                  .reduce((a,b) => a+b)
                  .value();

    fs.writeFileSync(module_name + '.ts', result);
}

export function tmtac(): void {
    var module_name = get_module_name_from_argv('tmtac');
    var filepaths = get_filepaths(module_name);
    var last_indices = _.chain(filepaths)
            .map(read_file)
            .map(x => x.split('\n').length - 1)
            .reduce((memo, num) => memo.concat(memo.slice(-1)[0] + num), [0])
            .value();

    // "Translate" a line number in the concatenated module file into the
    // corresponding filepath and line number before concatenation.
    function translate_line_number(n:number): [string, number] {
        if (n < 1 || n > last_indices.slice(-1)[0]) {
            return ['', 0];
        }
        for (var i = 0; i < last_indices.length; i++) {
            if (last_indices[i] >= n) break;
        }
        return [filepaths[i - 1], n - last_indices[i - 1]];
    }

    var error_found = false;

    for_each_line_in_stdin_async(tsc_message => {
        try {
            var message = translate_message(tsc_message, module_name, translate_line_number);
            console.log(message);
        } catch (e) {
            if (e !== Foo) {
                throw e;
            }
            console.log('* ' + tsc_message);
            error_found = true;
        }
    },
    () => {
        if (error_found) {
            console.log('\ntmtac: Lines marked with an asterisk (*) ' +
                'indicate that something has gone wrong.')
                // TODO give a real explanation
        }
    });
}

function translate_message(tsc_message:string, module_name:string, translate_line_number:Function): string {
    // Each line of tsc's output is one error message, of this form:
    // filename.ts(line_num, col_num): description
    var message_prefix = module_name + '.ts(';
    if (_s.startsWith(tsc_message, message_prefix)) {
        // Parse into line number and everything else.
        var after_filename = tsc_message.slice(message_prefix.length);
        var pattern = /^(\d+)(,.*)$/;
        var match = after_filename.match(pattern);
        if (match === null) {
            throw Foo;
        }
        var module_line_num = match[1];
        var message_suffix = match[2];

        // Translate.
        var __ = translate_line_number(module_line_num);
        var filepath = __[0];
        var line_num = __[1];
        if (line_num === 0) {
            throw Foo;
        }

        // Reassemble message.
        return filepath + '(' + line_num + message_suffix;
    } else {
        return tsc_message;
    }
}

function get_module_name_from_argv(caller:string): string {
    if (process.argv.length === 3) {
        var dir_name = process.argv[2];
        check_directory_exists(dir_name, caller);
        return dir_name;
    } else {
        console.error(caller + ': Invalid number of arguments.\n' +
            'Usage: ' + caller + ' [module name]\n' +
            '\n' +
            'man ' + caller + ' for details.'); // TODO: make man pages
        process.exit(1);
    }
}

// Call f(line) for each line in standard input.
// After this is done, call after().
function for_each_line_in_stdin_async(f:Function, after:Function): void {
    // Adapted from https://github.com/joyent/node/issues/7412
    // This issue is also the reason why this is an asynchronous function.
    var chunks = [];

    process.stdin
        .on("data", chunk => chunks.push(chunk))
        .on("end", () => {
            var lines = chunks.join('').split('\n').slice(0, -1);
            _.each(lines, x => f(x));
            after();
        });
}

function get_filepaths(module_name:string): string[] {
    var filenames = fs.readdirSync(module_name);
    return _.map(filenames,
                 name => module_name + '/' + name);
}

function read_file(path): string {
    // TODO: check that this is a file (and not, e.g., a directory)?
    return fs.readFileSync(path, 'utf8');
}

function check_directory_exists(path:string, caller:string): void {
    if (!directory_exists(path)) {
        console.error(caller + ': Directory ' + path + ' does not exist.');
        process.exit(1);
    }
}

function directory_exists(path:string): boolean {
    try {
        return fs.lstatSync(path).isDirectory();
    } catch (e) {
        if (e.code !== 'ENOENT') {
            throw e;
        }
        return false;
    }
}