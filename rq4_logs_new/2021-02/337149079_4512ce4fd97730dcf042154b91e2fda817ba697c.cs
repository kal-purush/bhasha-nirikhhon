using System;
using System.Collections.Generic;
using System.Text;

namespace SpaceGame
{
    class FuelEvent : RandomEvent
    {
        // Property getters scale ammount of fuel lost/gained with difficulty
        int fuelLost => new Random().Next(difficulty / 3, difficulty * 2);
        int fuelGained => new Random().Next(difficulty, difficulty * 2);

        // Use the base class constructors
        public FuelEvent() : base() { }
        public FuelEvent(int difficulty) : base(difficulty) { }

        // Take away some hull integrity or fuel and display a message
        public override void NegativeEvent(Ship ship)
        {
            ship.Fuel -= fuelLost;
            Console.WriteLine(GetNegativeEventMessage());
        }

        // Add some hull integrity or fuel and display a message
        public override void PositiveEvent(Ship ship)
        {
            ship.Fuel += fuelGained;
            Console.WriteLine(GetPositiveEventMessage());
        }

        public override string GetNegativeEventMessage()
        {
            // Some negative travel event messages
            string[] fuelMessages = new string[]
            {
                $"You fell asleep in the cockpit and have hit a small asteroid! The window cracks, causing {fuelLost} damage to the hull.",
                $"You took fire from a group of orbital space bandits! -{fuelLost} hull integrity.",
                $"Whoops! Someone left the window down and your toolbox flew out the window. -{fuelLost} hull integrity.",
                $"Before launching, you notice that a pack of alien bandits has vandalised your ship. -{fuelLost} hull integrity."
            };

            return fuelMessages[new Random().Next(0, fuelMessages.Length)];
        }

        public override string GetPositiveEventMessage()
        {
            // Some positive hull event messages
            string[] fuelMessages = new string[]
            {
                $"Bored on your way to the next planet, you read about a new trick in the Ship Mechanics Weekly magazine, and after following a tutorial, restore {fuelGained} hull integrity!",
                $"You find some spare WD-40 in a cabinet, and decide to lube up some of the blasters, restoring {fuelGained} hull integrity.",
                $"You discover a free ship repair coupon in your wallet! You redeem it at a nearby SpaceLube and restore {fuelGained} hull integrity.",
                $"Your shield generator starts working again for no apparant reason! +{fuelGained} hull integrity."
            };

            // Return a random message
            return fuelMessages[new Random().Next(0, fuelMessages.Length)];
        }
    }
}