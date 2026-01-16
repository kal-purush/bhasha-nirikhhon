using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Twitch_API.Twitch
{
    class TwitchGame
    {
        private string game_name = "";

        public TwitchGame(Dictionary<string, object> game_dict)
        {
            this.createGames(game_dict);            
        }
        
        public void createGames(Dictionary<string, object> game_dict)
        {
            foreach (string key in game_dict.Keys)
            {
                switch (key)
                {
                    case "name":
                        this.game_name = (string)game_dict[key];
                        break;
                }
            }

        }

        public string Game
        {
            get 
            {
                return game_name;
            }
        }
    }
}