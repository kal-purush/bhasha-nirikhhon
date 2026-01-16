import irc = require('irc');
import ServerStatus = require('../../Commands/ServerStatus');

describe('A Server Status Command', () =>
{
    describe('A Build Irc Message method', () =>
    {
        it('should return the correct message if the server is up', () =>
        {
            // Arrange
            var from: string = 'Noble';
            var ircClient: irc.Client = null;
            var host: string = 'test';
            var serverStatus: ServerStatus.ServerStatus = new ServerStatus.ServerStatus(ircClient, host);

            // Act
            var message = serverStatus.BuildIrcMessage(true, from);

            // Assert
            expect(message).toEqual('Hi der Noble test looks up from here!');
        });

        it('should return the correct message if the server is down', () =>
        {
            // Arrange
            var from: string = 'Noble';
            var ircClient: irc.Client = null;
            var host: string = 'test';
            var serverStatus: ServerStatus.ServerStatus = new ServerStatus.ServerStatus(ircClient, host);

            // Act
            var message = serverStatus.BuildIrcMessage(false, from);

            // Assert
            expect(message).toEqual('Hi der Noble test looks down from here!');
        });
    });
});