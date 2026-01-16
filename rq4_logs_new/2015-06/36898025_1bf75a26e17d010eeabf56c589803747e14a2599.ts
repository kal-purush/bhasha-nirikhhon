/// <reference path="../../DefinitelyTyped/jasmine.d.ts" />
/// <reference path="../../DefinitelyTyped/irc.d.ts" />

import irc = require('irc');
import CommandRouter = require('../../Core/CommandRouter');
import ServerStatus = require('../../Commands/ServerStatus');
import VehiclePrices = require('../../Commands/VehiclePrices');

describe('A Vehicle Prices Command', () =>
{
    describe('A IsValidVehicle method', () =>
    {
        it('should return true if the vehicle is valid', () =>
        {
            // Arrange
            var vehicleName: string = 'Sultan';
            var ircClient: irc.Client = null;
            var vehicleCommand = new VehiclePrices.VehiclePrices(ircClient);

            // Act
            var result = vehicleCommand.GetVehicle(vehicleName);

            // Assert
            expect(result).not.toBeNull();
            expect(result).toEqual('Sultan');
        });

        it('should return false if the vehicle is invalid', () =>
        {
            // Arrange
            var vehicleName: string = 'Ford';
            var ircClient: irc.Client = null;
            var vehicleCommand = new VehiclePrices.VehiclePrices(ircClient);

            // Act
            var result = vehicleCommand.GetVehicle(vehicleName);

            // Assert
            expect(result).not.toBeNull();
            expect(result).toEqual('');
        });
    });

    describe('A Build Irc Message method', () =>
    {
        it('should return the correct message if the vehicle is valid', () =>
        {
            // Arrange
            var vehicleName: string = 'Sultan';
            var ircClient: irc.Client = null;
            var from: string = 'Noble';
            var to: string = null;
            var vehicleCommand = new VehiclePrices.VehiclePrices(ircClient);

            // Act
            var result = vehicleCommand.BuildIrcMessage(from, to, vehicleName);

            // Assert
            expect(result).not.toBeNull();
            expect(result).toEqual('So Noble, according to my trusty records a Sultan costs $785,000.');
        });

        it('should return the correct message if the vehicle is invalid', () =>
        {
            // Arrange
            var vehicleName: string = 'Ford';
            var ircClient: irc.Client = null;
            var from: string = 'Noble';
            var to: string = null;
            var vehicleCommand = new VehiclePrices.VehiclePrices(ircClient);

            // Act
            var result = vehicleCommand.BuildIrcMessage(from, to, vehicleName);

            // Assert
            expect(result).not.toBeNull();
            expect(result).toEqual('Oh noes Noble, I couldn\'t find that vehicle!');
        });
    });
});