using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ArmadaEngine.UI;
using ArmadaEngine.Scenes.Sagey.Managers;
using Microsoft.Xna.Framework;
using ArmadaEngine.Scenes.Sagey.UI.Bank;

namespace ArmadaEngine.Scenes.Sagey.UI
{
    class CraftPanel : UIPanel
    {
        public event EventHandler BankChanged;

        ChemistryManager _ChemManager;
        BankItemsContainer ItemsContainer;
        public CraftPanel(UIManager uim, ChemistryManager cm) : base(uim)
        {
            _ChemManager = cm;
            _Name = "Crafting";
        }

        public override void LoadContent(string name)
        {
            base.LoadContent(name);

        }

        public override void Setup()
        {
            base.Setup();
        }

        public override void ProcessClick(Vector2 pos)
        {
            base.ProcessClick(pos);
            List<CraftingSlot> activeSlots = CraftSlots.FindAll(x => x.Active);
            foreach (CraftingSlot slot in activeSlots)
            {
                if (slot.MyRect.Contains(pos))
                {
                    _ChemistryManager.ProcessRecipe(slot.MyRecipe);
                    break;
                }
            }
        }

        public void HandleBankChanged(object sender, EventArgs args)
        {
            if (!this._Show) return;
            //foreach (Inventory.ItemListItem ili in ItemsContainer.InvenItems)
            //{
            //    ili.Reset();
            //}

            //int c = 0;
            //foreach (GameObjects.ItemSlot s in _BankManager.itemSlots)
            //{
            //    ItemsContainer.InvenItems[c].SetItem(s);
            //    c++;
            //}
        }
    }
}