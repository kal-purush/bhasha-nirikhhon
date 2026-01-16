import {CommandBuilder} from './command.builder';
import {expect} from 'chai';
import 'mocha';

describe('CommandBuilder', () => {
    it('should render a command with no options and no arguments', function() {
        const builder = new CommandBuilder();
        builder.command = '/usr/bin/node';
        const result = builder.render();
        expect(result).to.equal('/usr/bin/node');
    });

    it('should render a command with options and no arguments', function() {
        const builder = new CommandBuilder();
        builder.command = '/usr/bin/node';
        builder.options.push({
            name: '--path',
            value: '/bin',
            useEqualSign: true,
        });
        builder.options.push({
            name: '--path',
            value: '/bin',
            useEqualSign: false,
        });
        builder.options.push({
            name: '--verbose',
        });

        builder.options.push({
            name: '--log-level',
            value: 'debug',
        });

        const result = builder.render();
        expect(result).to.equal('/usr/bin/node --path="/bin" --path "/bin" --verbose --log-level "debug"');
    });

    it('should render a command with no options and arguments', function() {
        const builder = new CommandBuilder();
        builder.command = '/usr/bin/node';

        builder.arguments.push('index.js');
        builder.arguments.push('--');
        builder.arguments.push('--version');

        const result = builder.render();
        expect(result).to.equal('/usr/bin/node "index.js" "--" "--version"');
    });

    it('should render a command with options and arguments', function() {
        const builder = new CommandBuilder();
        builder.command = '/usr/bin/node';
        builder.options.push({
            name: '--path',
            value: '/bin',
            useEqualSign: true,
        });
        builder.options.push({
            name: '--path',
            value: '/bin',
            useEqualSign: false,
        });
        builder.options.push({
            name: '--verbose',
        });

        builder.options.push({
            name: '--log-level',
            value: 'debug',
        });

        builder.arguments.push('index.js');
        builder.arguments.push('--');
        builder.arguments.push('--version');

        const result = builder.render();
        expect(result).to.equal('/usr/bin/node --path="/bin" --path "/bin" --verbose --log-level "debug" "index.js" "--" "--version"');
    });

    it('should escape option values and arguments', function() {
        const builder = new CommandBuilder();
        builder.command = '/usr/bin/node';

        builder.options.push({
            name: '--environment',
            value: 'foo"bar"',
            useEqualSign: true,
        });

        builder.arguments.push('foo"bar"');

        const result = builder.render();
        expect(result).to.be.equal('/usr/bin/node --environment="foo\\"bar\\"" "foo\\"bar\\""');
    });

    it('should work when commands are chained together', function() {
        let builder = new CommandBuilder();
        builder.command = '/usr/bin/node';
        builder.arguments.push('index.js');
        let result = builder.render();
        builder = new CommandBuilder();
        builder.command = '/usr/bin/ssh';
        builder.arguments.push('web.int.datenknoten.me');
        builder.arguments.push(result);

        result = builder.render();
        expect(result).to.be.equal('/usr/bin/ssh "web.int.datenknoten.me" "/usr/bin/node \\"index.js\\""');
    });

    it('should have some environment variables for command', function() {
        const builder = new CommandBuilder();
        builder.command = '/usr/bin/node';
        builder.arguments.push('index.js');

        builder.env.foobar = 'blub';
        builder.env.barfoo = 'foo"bar"';

        const result = builder.render();
        expect(result).to.be.equal('foobar="blub" barfoo="foo\\"bar\\"" /usr/bin/node "index.js"');
    });
});