import { execWithArguments } from '../support.spec';

describe('serve command', function () {
    it('should show usage when no command is specified', function () {
        return execWithArguments('').then(({stderr}) => stderr.should.contain('Usage: '));
    });

    it('should show an error when no recognized command is specified', function () {
        return execWithArguments('foo').then(({stderr}) => stderr.should.contain('Unknown argument: foo'));
    });

    it('should show an error when no path is specified', function () {
        return execWithArguments('serve').then(({stderr}) => stderr.should.contain('path must be a string'));
    });

    it('should show an error when the specified path does not exist', function () {
        return execWithArguments('serve foo').then(({stderr}) => stderr.should.contain('no such file or directory'));
    });

    it('should start a server on port 4000 when the schema exists', function () {
        return execWithArguments('serve test/schema.graphql').then(({stdout}) => {
            stdout.should.contain(`port: '4000'`);
            stdout.should.not.contain('GraphiQL');
        });
    });

    it('should start a server on another port when the --port option is used', function () {
        return execWithArguments('serve test/schema.graphql --port 5000') .then(({stdout}) => {
            stdout.should.contain(`port: '5000'`);
            stdout.should.not.contain('GraphiQL');
        });
    });

    it('should start a server on another port when the -p option is used', function () {
        return execWithArguments('serve test/schema.graphql -p 5000').then(({stdout}) => {
            stdout.should.contain(`port: '5000'`);
            stdout.should.not.contain('GraphiQL');
        });
    });

    it('should start a server and not deploy GraphiQL when the --graphiql option is false', function () {
        return execWithArguments('serve test/schema.graphql --graphiql false').then(({stdout}) => {
            stdout.should.contain(`port: '4000'`);
            stdout.should.contain('GraphiQL');
        });
    });

    it('should start a server and not deploy GraphiQL when the --graphiql option is false', function () {
        return execWithArguments('serve test/schema.graphql --g false').then(({stdout}) => {
            stdout.should.contain(`port: '4000'`);
            stdout.should.contain('GraphiQL');
        });
    });
});