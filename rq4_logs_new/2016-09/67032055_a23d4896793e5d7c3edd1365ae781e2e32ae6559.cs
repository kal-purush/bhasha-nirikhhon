using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NemesesGame
{
    public class RefResources
    {
        public RefResources()
        {
            StartingResourcesCount();
            ResourcesRegenBasicCount();
        }

        // Starting Resources
        int goldStarting = 2000;
        int woodStarting = 250;
        int stoneStarting = 100;
        int ironStarting = 50;
        Resources startingResources;

        // Resource Production Rate : Basic
        int goldRegenBasic = 2000;
        int woodRegenBasic = 50;
        int stoneRegenBasic = 20;
        int ironRegenBasic = 0;
        Resources resourcesRegenBasic;

        // Resource Production Rate : Improved
        int woodRegenImproved = 100;
        int stoneRegenImproved = 60;
        int ironRegenImproved = 30;

        // Resource Production Rate : Advanced
        int woodRegenAdv = 300;
        int stoneRegenAdv = 120;
        int ironRegenAdv = 100;

        // Public Getter ------------------------------------------------------------

        public int GoldStarting { get { return goldStarting; } }
        public int WoodStarting { get { return woodStarting; } }
        public int StoneStarting { get { return stoneStarting; } }
        public int IronStarting { get { return ironStarting; } }
        void StartingResourcesCount()
        {
            startingResources.Gold = goldStarting;
            startingResources.Wood = woodStarting;
            startingResources.Stone = stoneStarting;
            startingResources.Iron = ironStarting;
        }
        public Resources StartingResources { get { return startingResources; } }

        public int GoldRegenBasic { get { return goldRegenBasic; } }
        public int WoodRegenBasic { get { return woodRegenBasic; } }
        public int StoneRegenBasic { get { return stoneRegenBasic; } }
        public int IronRegenBasic { get { return ironRegenBasic; } }
        void ResourcesRegenBasicCount()
        {
            resourcesRegenBasic.Gold = goldRegenBasic;
            resourcesRegenBasic.Wood = woodRegenBasic;
            resourcesRegenBasic.Stone = stoneRegenBasic;
            resourcesRegenBasic.Iron = ironRegenBasic;
        }
        public Resources ResourcesRegenBasic { get { return resourcesRegenBasic; } }

        public int WoodRegenImproved { get { return woodRegenImproved; } }
        public int StoneRegenImproved { get { return stoneRegenImproved; } }
        public int IronRegenImproved { get { return ironRegenImproved; } }

        public int WoodRegenAdv { get { return woodRegenAdv; } }
        public int StoneRegenAdv { get { return stoneRegenAdv; } }
        public int IronRegenAdv { get { return ironRegenAdv; } }

        public void WoodImproved(int thisWood) { thisWood = woodRegenBasic; }
    }
}