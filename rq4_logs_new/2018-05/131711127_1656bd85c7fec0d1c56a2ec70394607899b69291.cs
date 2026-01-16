using DockService.Core.Messaging;
using DockService.Core.Models;
using DockService.Core.Services;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Utf8Json;

namespace DockService.App.Messaging
{
    public class DockEventHandler : IEventHandlerCallback
	{
		private readonly IDockService _dockService;
        public DockEventHandler(IDockService dockService)
        {
            _dockService = dockService;
        }

        #region --- main stuff---
        public async Task<bool> HandleEventAsync(EventTypes eventType, string message)
        {
            switch (eventType)
            {
                case EventTypes.ShipNearingHarbor:
                    {
                        return await HandleShipNearingHarbor(message);
                    }
                case EventTypes.ShipUndock:
                    {
                        return await HandleShipUndock(message);
                    }
               /* case EventTypes.ShipDocked:
                    {
                        return await HandleShipDocked(message);
                    }*/
                case EventTypes.ShipUndocked:
                    {
                        return await HandleShipUndocked(message);
                    }
            }

            return true;
        }
        #endregion

        #region --- Incoming Dock and Undock events ---
        private async Task<bool> HandleShipNearingHarbor(string message)
        {
            Console.WriteLine("entered function");
            Ship receivedShip = JsonSerializer.Deserialize<Ship>(message);

            // set enum
            receivedShip.DockingStatus = DockingStatus.ShipDockingRequested;
            /*
            // TODO: save in repo?
            Ship createdShip = await _dockService.CreateShipAsync(receivedShip);
            */
          
            Task.Run(() => _dockService.SendShipDockedAsync(receivedShip));//(createdship) )TODO send shipdocked

            return true;
        }

        private async Task<bool> HandleShipUndock(string message)
        {
          
            Ship receivedShip = JsonSerializer.Deserialize<Ship>(message);

            // IF i decide to use db:
            /*
            // get ship
            Ship existingShip = await _dockService.GetShipAsync(receivedShip.Serial);
            existingShip.DockStatus = DockStatus.NotStarted;

            //save ship
            await _dockService.SaveShipAsync(existingShip);
            */
         
            Task.Run(() => _dockService.SendShipUndockedAsync(receivedShip));//existingShip

            return true;
        }
        #endregion

        #region --- Tugboat evens ---
        private async Task<bool> HandleTugboatDispatched(string message)
        {

            //TODO not yet sure how/what im going to do here
           /* Ship receivedShip = JsonSerializer.Deserialize<Ship>(message);
            if (receivedShip.ShipType == ShipType.Tugboat)
            {
                Task.Run(() => _dockService.SendTugboatDispatchedAsync(receivedShip));
             }
             */
            
            

            return true;
        }
        #endregion

        #region ---Outgoing events---
        //Not sure if they should be handled here, probably not
        private async Task<bool> HandleShipDocked(string message)
        {
            
            Ship dockedShip = JsonSerializer.Deserialize<Ship>(message);
            
            // send event y/n?
           // await _dockService.SendShipDockedEventAsync(dockedShip.serial);

            return true;
        }

        private async Task<bool> HandleShipUndocked(string message)
        {
           
            Ship undockedShip = JsonSerializer.Deserialize<Ship>(message);

           
            await _dockService.SendShipUndockedAsync(undockedShip);

            return true;
        }
        #endregion

    }
}