using System;
using System.Collections.Generic;
using System.Text;
using SpaceGame.Ship;

namespace SpaceGame
{
    internal class Planet
    {
        string PlanetName { get; set; }
        string PlanetResource { get; set; }
        int PlanetMultiplier { get; set; }
        (int, int) PlanetCords { get; set; }
        int PlanetGoldCost { get; set; }
        int PlanetFuelCost { get; set; }
        int PlanetHullCost { get; set; }

        internal Planet(string name, string resource, int multiplier, (int x, int y) cords, int goldCost, int fuelCost, int hullCost)
        {
            this.PlanetName = name;
            this.PlanetResource = resource;
            this.PlanetMultiplier = multiplier;
            this.PlanetCords = cords;
            this.PlanetGoldCost = goldCost;
            this.PlanetFuelCost = fuelCost;
            this.PlanetHullCost = hullCost;
        }
        internal double DistanceToShip(int x, int y) //returns difference of caller and current planet
        {
            double z = Math.Sqrt((x * x) + (y * y));
            (int px, int py) = PlanetCords;
            double pz = Math.Sqrt((px * px) + (py * py));
            return Math.Abs(z - pz);
        }
        internal string ShowStore()
        {
            return $"{ShipTime} days remaining.\n\nYou have the following resorces:\n{ShipGold} gold.\n{ShipFuel} fuel.\n{ShipHull}/{ShipHullMax} hull integrity." +
                $"\n\n\nGold is available for {PlanetGoldCost} coin\nFuel is available for {PlanetFuelCost} coin\nHull repair parts are avable for {PlanetHullCost} coin";
        }

        internal void Sell(string switchCase, int amount)
        {
            switch (switchCase)
            {
                case "1":
                    ShipGold -= amount;
                    ShipCoin += (amount * PlanetGoldCost) / 2;
                case "2":
                    ShipFuel -= amount;
                    ShipCoin += (amount * PlanetFuelCost) / 2;
                    break;
                case "3":
                    ShipHull -= amount;
                    ShipCoin += (amount * PlanetHullCost) / 2;
                    break;
            }
        }
        internal void Buy(string switchCase, int amount)
        {
            switch (switchCase)
            {
                case "1":
                    ShipGold += amount;
                    ShipCoin -= amount * PlanetGoldCost;
                case "2":
                    ShipFuel += amount;
                    ShipCoin -= amount * PlanetFuelCost;
                    break;
                case "3":
                    ShipHull += amount;
                    ShipCoin -= amount * PlanetHullCost;
                    break;
            }
        }
        internal void Mine()
        {

        }
    }
}