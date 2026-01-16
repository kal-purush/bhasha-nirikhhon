namespace LoZClone
{
    using System.Collections.Generic;
    using System.IO;

    public class Profiles
    {
        private List<string> fileSave;

        public Profiles()
        {
            fileSave = new List<string>();
        }

        public void WriteToSaveFile()
        {
            fileSave.Add(LoZGame.Instance.Dungeon.DungeonNumber.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Health.CurrentHealth.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Health.MaxHealth.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].CurrentWeapon.ToString());
            fileSave.Add(GameData.Instance.InventoryConstants.MaximumBombs.ToString());
            fileSave.Add(GameData.Instance.InventoryConstants.MaximumSelectionX.ToString());
            fileSave.Add(GameData.Instance.InventoryConstants.MaximumSelectionY.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.SelectionX.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.SelectionY.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.Bombs.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.Keys.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.RedPotions.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.BluePotions.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.Rupees.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasBoomerang.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasRod.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasMagicBoomerang.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasBow.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasArrow.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasSilverArrow.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasBlueFlame.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasRedFlame.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasFlute.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasLadder.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.LadderInUse.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasMap.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasCompass.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.HasClock.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.ClockLockout.ToString());
            fileSave.Add(LoZGame.Instance.Players[0].Inventory.SelectedItem.ToString());

            TextWriter saveFile = new StreamWriter("../../../../etc/profiles/Profile#" + LoZGame.Instance.SelectedProfile + ".txt");

            foreach (string element in fileSave)
            {
                saveFile.WriteLine(element);
            }

            saveFile.Close();
        }

        public void ResetSaveFile()
        {
            fileSave.Add(1.ToString());
            fileSave.Add(GameData.Instance.PlayerConstants.StartingHealth.ToString());
            fileSave.Add(20.ToString());
            fileSave.Add(Link.LinkWeapon.Wood.ToString());
            fileSave.Add(GameData.Instance.InventoryConstants.MaximumBombs.ToString());
            fileSave.Add(GameData.Instance.InventoryConstants.MaximumSelectionX.ToString());
            fileSave.Add(GameData.Instance.InventoryConstants.MaximumSelectionY.ToString());
            fileSave.Add(0.ToString());
            fileSave.Add(0.ToString());
            fileSave.Add(4.ToString());
            fileSave.Add(0.ToString());
            fileSave.Add(0.ToString());
            fileSave.Add(0.ToString());
            fileSave.Add(5.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(false.ToString());
            fileSave.Add(0.ToString());
            fileSave.Add(InventoryManager.ItemType.Bomb.ToString());

            TextWriter saveFile = new StreamWriter("../../../../etc/profiles/Profile#" + LoZGame.Instance.SelectedProfile + ".txt");

            foreach (string element in fileSave)
            {
                saveFile.WriteLine(element);
            }

            saveFile.Close();
        }

        public Link.LinkWeapon ParseWeapon(IPlayer player, string weapon)
        {
            if (weapon.Equals("Magic"))
            {
                return Link.LinkWeapon.Magic;
            }
            else if (weapon.Equals("White"))
            {
                return Link.LinkWeapon.White;
            }
            else
            {
                return Link.LinkWeapon.Wood;
            }
        }

        public InventoryManager.ItemType ParseSelectedItem(string item)
        {
            if (item.Equals("Bomb"))
            {
                return InventoryManager.ItemType.Bomb;
            }
            else if (item.Equals("Arrow"))
            {
                return InventoryManager.ItemType.Arrow;
            }
            else if (item.Equals("Boomerang"))
            {
                return InventoryManager.ItemType.Boomerang;
            }
            else if (item.Equals("Candle"))
            {
                return InventoryManager.ItemType.Candle;
            }
            else if (item.Equals("Flute"))
            {
                return InventoryManager.ItemType.Flute;
            }
            else if (item.Equals("Potion"))
            {
                return InventoryManager.ItemType.Potion;
            }
            else if (item.Equals("Rod"))
            {
                return InventoryManager.ItemType.Rod;
            }
            else
            {
                return InventoryManager.ItemType.None;
            }
        }
    }
}