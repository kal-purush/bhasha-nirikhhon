using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TMPro;
using UnityEngine;

using static RandomizerMod.LogHelper;

using RandomizerLib;

namespace RandomizerMod.MultiWorld
{
    // Utility class for displaying RelicMsg/Trinket flash
    static class RelicMsg
    {
        public static void ShowRelic(string spriteKey, string text)
        {
            GameObject relicMsg = ObjectCache.RelicMsg;

            SpriteRenderer renderer = relicMsg.GetComponentInChildren<SpriteRenderer>();
            TextMeshPro tmp = relicMsg.GetComponentInChildren<TextMeshPro>();

            renderer.sprite = RandomizerMod.GetSprite(spriteKey);
            tmp.text = text;

            // Apparently Spawn extension is weird?
            UnityEngine.Object.Destroy(relicMsg.Spawn());
            relicMsg.SetActive(true);
        }

        public static void ShowRelicKey(string spriteKey, string nameKey)
        {
            ShowRelic(spriteKey, LanguageStringManager.GetLanguageString(nameKey, "UI"));
        }

        public static void ShowRelicItem(string item, string from)
        {
            item = LogicManager.RemovePrefixSuffix(item);
            ReqDef def = LogicManager.GetItemDef(item);

            ShowRelic(def.shopSpriteKey, $"{LanguageStringManager.GetLanguageString(def.nameKey, "UI")} from {from}");
        }
    }
}