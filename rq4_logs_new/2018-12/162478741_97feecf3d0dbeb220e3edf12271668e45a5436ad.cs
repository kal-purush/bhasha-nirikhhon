using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using ItemAPI;

namespace CustomItems
{
    class ChestReroller : PlayerItem
    {
        private Chest rerollChest;
        private string slotRoll = "Play_OBJ_Chest_Synergy_Slots_01";
        private static Material rainbowSheen = new Material(ShaderCache.Acquire("Brave/Internal/RainbowChestShader"));

        public static void Init()
        {
            //The name of the item
            string itemName = "D-Chest"; //The name of the item
            string resourceName = "CustomItems/Resources/d_chest"; //Refers to an embedded png in the project. Make sure to embed your resources!

            //Generate a new GameObject with a sprite component
            GameObject spriteObj = ItemBuilder.CreateSpriteObject(itemName, resourceName);

            //Add a PassiveItem component to the object
            var item = spriteObj.AddComponent<ChestReroller>();

            //Ammonomicon entry variables
            string shortDesc = "Reroll Chests";
            string longDesc = "Rerolls chests.\n\nWe're like 20% sure this is one of Daisuke's nephews";

            //Adds the item to the gungeon item list, the ammonomicon, the loot table, etc.
            ItemBuilder.SetupItem(item, shortDesc, longDesc, "kts");

            //Set the cooldown type and duration of the cooldown
            ItemBuilder.SetCooldownType(item, ItemBuilder.CooldownType.Damage, 1000f);

            //Adds a passive modifier, like curse, coolness, damage, etc. to the item. Works for passives and actives.
            //ItemBuilder.AddPassiveStatModifier(item, PlayerStats.StatType.Coolness, 1f);

            //Set some other fields
            item.consumable = false;
            item.quality = ItemQuality.B;
        }

        protected override void DoEffect(PlayerController user)
        {
            IPlayerInteractable nearestInteractable = user.CurrentRoom.GetNearestInteractable(user.CenterPosition, 1f, user);
            if (!(nearestInteractable is Chest)) return;

            Chest rerollChest = nearestInteractable as Chest;
            if (rerollChest.IsMimic)
            {
                rerollChest.ForceOpen(user);
                return;
            }

            AkSoundEngine.PostEvent(slotRoll, base.gameObject);
            //replaceChest.majorBreakable.TemporarilyInvulnerable = true;
            if (rerollChest.GetComponent<MeshRenderer>() != null)
                MakeRainbow(rerollChest.GetComponent<MeshRenderer>());
            this.rerollChest = rerollChest;
            StartCoroutine(ItemBuilder.HandleDuration(this, 1.25f, user, ReplaceChest));
        }

        public void ReplaceChest(PlayerController user)
        {
            if (rerollChest == null || rerollChest.IsBroken) return;

            Chest newChest = GameManager.Instance.RewardManager.SpawnRewardChestAt(rerollChest.sprite.WorldBottomLeft.ToIntVector2());
            user.CurrentRoom.DeregisterInteractable(rerollChest);
            rerollChest.DeregisterChestOnMinimap();
            Destroy(rerollChest.gameObject);
        }

        public void MakeRainbow(MeshRenderer renderer)
        {
            Material[] sharedMaterials = renderer.sharedMaterials;
            Array.Resize<Material>(ref sharedMaterials, sharedMaterials.Length + 1);

            Material material = UnityEngine.Object.Instantiate<Material>(rainbowSheen);
            material.SetTexture("_MainTex", sharedMaterials[0].GetTexture("_MainTex"));
            sharedMaterials[sharedMaterials.Length - 1] = material;

            renderer.sharedMaterials = sharedMaterials;
        }

        public override bool CanBeUsed(PlayerController user)
        {
            IPlayerInteractable nearestInteractable = user.CurrentRoom.GetNearestInteractable(user.CenterPosition, 1f, user);
            return nearestInteractable is Chest;
        }
    }
}