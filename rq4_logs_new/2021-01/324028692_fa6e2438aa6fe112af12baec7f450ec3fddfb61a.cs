using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Modding;
using RandomizerLib.MultiWorld;

namespace RandomizerLib
{
    [Serializable]
    public class RandoResult
    {
        public int playerId;
        public RandoSettings settings;
        public List<string> startItems;
        public Dictionary<MWItem, string> itemPlacements;       // item -> location
        public Dictionary<string, string> transitionPlacements; // transition -> transition
        public Dictionary<MWItem, int> shopCosts;               // MWItem (in shop) -> shop cost
        public Dictionary<string, int> variableCosts;           // grub/essence location -> randomized grub/essence count

        public RandoResult()
        {
            playerId = -1;
            settings = new RandoSettings();
            startItems = new List<string>();
            itemPlacements = new Dictionary<MWItem, string>();
            transitionPlacements = new Dictionary<string, string>();
            shopCosts = new Dictionary<MWItem, int>();
            variableCosts = new Dictionary<string, int>();
        }
    }
}