﻿using System;
using SharpDX;
using System.IO.MemoryMappedFiles;
using System.IO;


namespace Turbo.Plugins.Default
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Text.RegularExpressions;
    using SharpDX.Text;
    using System.Windows.Forms;

    public class SntsToolAdapter : BasePlugin, IBeforeRenderHandler, IInGameTopPainter
    {
        private MemoryMappedFile _mmf;
        private IFont watermark;
        private IMonster monsterToTarget;

        private const String SHARED_FILE = "SNTS_TOOL_SHARED_FILE";

        public SntsToolAdapter()
        {
            Enabled = true;
             _mmf = MemoryMappedFile.CreateNew(SHARED_FILE, 1000);
        }

        public override void Load(IController hud)
        {
            base.Load(hud);
        }

        private String GetMonsterInfo(IMonster monster) 
        {
            IScreenCoordinate screenCoord = monster.FloorCoordinate.ToScreenCoordinate();
            return GetMonsterPriority(monster) + ": " + monster.Attackable + " " + (int)monster.CentralXyDistanceToMe 
                + " " + monster.SnoMonster.NameEnglish 
                + "(" + (int)screenCoord.X + ", " + (int)screenCoord.Y + ")";
        }

        public void PaintTopInGame(ClipState clipState)
        { 
            /*
            var isInRift = Convert.ToString(Hud.Game.SpecialArea == SpecialArea.Rift || Hud.Game.SpecialArea == SpecialArea.GreaterRift);
            var numMonstens = Convert.ToString(Hud.Game.AliveMonsters.Count()).PadLeft(4).Replace(' ', '0');

            watermark = Hud.Render.CreateFont("tahoma", 10, 155, 200, 0, 0, true, false, false);
            watermark.DrawText(watermark.GetTextLayout(numMonstens), Hud.Window.Size.Width * 0.2f, Hud.Window.Size.Height * 0.5f);
            watermark.DrawText(
                monsterToTarget == null ? "" : GetMonsterInfo(monsterToTarget), 
                Hud.Window.Size.Width * 0.2f, Hud.Window.Size.Height * 0.53f);
            var offset = 0.44f;
            foreach (var elite in Hud.Game.AliveMonsters.Where(x => x.IsElite)) 
            {
                watermark.DrawText(
                    watermark.GetTextLayout(GetMonsterInfo(elite)), 
                    Hud.Window.Size.Width * 0.65f, Hud.Window.Size.Height * (offset));
                offset += 0.02f;
            }
            */
        }
        
        public void BeforeRender()
        {
            bool isHexingPantsEquipped = Hud.Game.Me.Powers.GetBuff(318817) == null
                ? false
                : Hud.Game.Me.Powers.GetBuff(318817).Active;
            bool isInRift = (Hud.Game.SpecialArea == SpecialArea.Rift || Hud.Game.SpecialArea == SpecialArea.GreaterRift);

            var hasMaxEssence = Hud.Game.Me.Stats.ResourceCurEssence == Hud.Game.Me.Stats.ResourceMaxEssence;
            HashSet<ActorSnoEnum> skeletonMageSNOs = new HashSet<ActorSnoEnum>
            {
                ActorSnoEnum._p6_necro_skeletonmage_a,
                ActorSnoEnum._p6_necro_skeletonmage_b,
                ActorSnoEnum._p6_necro_skeletonmage_e,
                ActorSnoEnum._p6_necro_skeletonmage_f_archer,
                ActorSnoEnum._p6_necro_skeletonmage_c,
                ActorSnoEnum._p6_necro_skeletonmage_d
            };
            var skeletonMageActors = Hud.Game.Actors.Where(actor => skeletonMageSNOs.Contains(actor.SnoActor.Sno) &&
                actor.SummonerAcdDynamicId == Hud.Game.Me.SummonerId);
            int numberOfSkeleMages = skeletonMageActors.Count();
            int numMonsters = Hud.Game.AliveMonsters.Count();

            


			IPlayerSkill skeletonMageSkill = Hud.Game.Me.Powers.UsedNecromancerPowers.SkeletalMage;
            IBuff skeletonMageBuff = null;
			if (skeletonMageSkill != null) {
                skeletonMageBuff = skeletonMageSkill.Buff;
            }
            int skeletonMageTimeLeft = 0;
            if (skeletonMageBuff != null)
            {
                // unused, only shows duration of last mage casted
                skeletonMageTimeLeft = (int) skeletonMageBuff.TimeLeftSeconds[5];
            }

            monsterToTarget = null;
            foreach (var monster in Hud.Game.AliveMonsters) 
            {
                if (!monster.Attackable || monster.CentralXyDistanceToMe > 60) 
                {
                    continue;
                }
                if (monsterToTarget == null) 
                {
                    monsterToTarget = monster;
                }
                if (
                    GetMonsterPriority(monster) > GetMonsterPriority(monsterToTarget) 
                    && monster.CurHealth > monsterToTarget.CurHealth
                    // TODO: shielding etc.
                    //&& !monster.Invulnerable
                    ) 
                {
                    monsterToTarget = monster;
                }
            }

            int targetX = 900, targetY = 500;
            bool hasTarget = monsterToTarget != null;
            int monsterPriority = 0;
            if (hasTarget) 
            {
                IScreenCoordinate screenCoord = monsterToTarget.FloorCoordinate.ToScreenCoordinate();
                targetX = (int) screenCoord.X;
                targetY = (int) screenCoord.Y;
                monsterPriority = GetMonsterPriority(monsterToTarget);
            }
            using (MemoryMappedViewStream stream = _mmf.CreateViewStream()) 
            {
                BinaryWriter writer = new BinaryWriter(stream);
                writer.Write(isHexingPantsEquipped);
                writer.Write(isInRift);
                writer.Write(hasMaxEssence);
                writer.Write(hasTarget);
                writer.Write(monsterPriority);
                writer.Write(targetX);
                writer.Write(targetY);
                writer.Write(skeletonMageTimeLeft);
                int secondsLeftToRecastMage = 4;
                int secondsMageDuration = 10;
                writer.Write(secondsLeftToRecastMage);
                writer.Write(secondsMageDuration);
                writer.Write(numberOfSkeleMages == null ? 0 : numberOfSkeleMages);
                writer.Write(numMonsters == null ? 0 : numMonsters);
            }
        }

        private int GetMonsterPriority(IMonster monster) 
        {
            if (monster.SnoMonster.Priority == MonsterPriority.boss)
            {
                return 3;
            }

            if (monster.Rarity == ActorRarity.Champion || monster.Rarity == ActorRarity.Rare)
            {
                return 2;
            }

            if (monster.Rarity == ActorRarity.RareMinion)
            {
                return 1;
            }
            
            return 0;
        }
    }
}