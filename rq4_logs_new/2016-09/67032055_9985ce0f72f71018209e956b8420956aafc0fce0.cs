using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NemesesGame;

namespace NemesesGame
{
    public class City
    {
        public Resources cityResources = new Resources(2000, 500, 200, 50);
        public PlayerDetails playerDetails = new PlayerDetails();

        /* Unimplemented yet
         * public Army cityArmy;
         * public Upgrades cityUpgrades;
         */

        public City(long TelegramId, string FirstName, string LastName)
        {
            playerDetails = new PlayerDetails(TelegramId, FirstName, LastName);
        }

        public string ThisPlayer()
        {
            PlayerDetails p = playerDetails;
            string s;
            s = p.firstName + " " + p.lastName + " (" + p.telegramId + ") has joined the game!";

            return s;
        }
    }
}