using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vintagestory.API;
using Vintagestory.API.Common;
using Vintagestory.API.Common.Entities;
using Vintagestory.API.Config;
using Vintagestory.API.Datastructures;
using Vintagestory.API.MathTools;

namespace Vintagestory.GameContent
{
    public class FixedEntityBehaviorMultiply : EntityBehavior
    {
        ITreeAttribute multiplyTree;
        JsonObject attributes;
        long callbackId;

        internal float PregnancyDays
        {
            get { return attributes["pregnancyDays"].AsFloat(3f); }
        }

        internal AssetLocation SpawnEntityCode
        {
            get { return new AssetLocation(attributes["spawnEntityCode"].AsString("")); }
        }

        internal string RequiresNearbyEntityCode
        {
            get { return attributes["requiresNearbyEntityCode"].AsString(""); }
        }

        internal float RequiresNearbyEntityRange
        {
            get { return attributes["requiresNearbyEntityRange"].AsFloat(5); }
        }

        public double MultiplyCooldownDaysMin
        {
            get { return attributes["multiplyCooldownDaysMin"].AsFloat(6); }
        }

        public double MultiplyCooldownDaysMax
        {
            get { return attributes["multiplyCooldownDaysMax"].AsFloat(12); }
        }

        public float PortionsEatenForMultiply
        {
            get { return attributes["portionsEatenForMultiply"].AsFloat(3); }
        }

        public float SpawnQuantityMin
        {
            get { return attributes["spawnQuantityMin"].AsFloat(1); }
        }
        public float SpawnQuantityMax
        {
            get { return attributes["spawnQuantityMax"].AsFloat(2); }
        }


        public double TotalDaysLastBirth
        {
            get { return multiplyTree.GetDouble("totalDaysLastBirth"); }
            set { multiplyTree.SetDouble("totalDaysLastBirth", value); }
        }

        public double TotalDaysPregnancyStart
        {
            get { return multiplyTree.GetDouble("totalDaysPregnancyStart"); }
            set { multiplyTree.SetDouble("totalDaysPregnancyStart", value); }
        }

        public double TotalDaysCooldownUntil
        {
            get { return multiplyTree.GetDouble("totalDaysCooldownUntil"); }
            set { multiplyTree.SetDouble("totalDaysCooldownUntil", value); }
        }

        public bool IsPregnant
        {
            get { return multiplyTree.GetBool("isPregnant"); }
            set { multiplyTree.SetBool("isPregnant", value); }
        }

        public bool ShouldEat
        {
            get
            {
                return
                    !IsPregnant
                    && GetSaturation() < PortionsEatenForMultiply
                    && TotalDaysCooldownUntil <= entity.World.Calendar.TotalDays
                ;
            }
        }

        public FixedEntityBehaviorMultiply(Entity entity) : base(entity)
        {

        }

        public override void Initialize(EntityProperties properties, JsonObject attributes)
        {
            base.Initialize(properties, attributes);

            this.attributes = attributes;
            multiplyTree = entity.WatchedAttributes.GetTreeAttribute("multiply");

            if (entity.World.Side == EnumAppSide.Server)
            {
                if (multiplyTree == null)
                {
                    entity.WatchedAttributes.SetAttribute("multiply", multiplyTree = new TreeAttribute());
                    TotalDaysLastBirth = entity.World.Calendar.TotalHours;

                    double daysNow = entity.World.Calendar.TotalHours / 24f;
                    TotalDaysCooldownUntil = daysNow + (MultiplyCooldownDaysMin + entity.World.Rand.NextDouble() * (MultiplyCooldownDaysMax - MultiplyCooldownDaysMin));
                }

                callbackId = entity.World.RegisterCallback(CheckMultiply, 3000);
            }
        }


        private void CheckMultiply(float dt)
        {
            if (!entity.Alive) return;

            callbackId = entity.World.RegisterCallback(CheckMultiply, 3000);

            if (entity.World.Calendar == null) return;

            double daysNow = entity.World.Calendar.TotalHours / 24f;

            if (!IsPregnant)
            {
                if (TryGetPregnant())
                {
                    IsPregnant = true;
                    TotalDaysPregnancyStart = daysNow;
                }

                return;
            }



            if (daysNow - TotalDaysPregnancyStart > PregnancyDays)
            {
                Random rand = entity.World.Rand;

                float q = SpawnQuantityMin + (float)rand.NextDouble() * (SpawnQuantityMax - SpawnQuantityMin);
                TotalDaysLastBirth = daysNow;
                TotalDaysCooldownUntil = daysNow + (MultiplyCooldownDaysMin + rand.NextDouble() * (MultiplyCooldownDaysMax - MultiplyCooldownDaysMin));
                IsPregnant = false;
                entity.WatchedAttributes.MarkPathDirty("multiply");
                EntityProperties childType = entity.World.GetEntityType(SpawnEntityCode);

                int generation = entity.WatchedAttributes.GetInt("generation", 0);

                while (q > 1 || rand.NextDouble() < q)
                {
                    q--;
                    Entity childEntity = entity.World.ClassRegistry.CreateEntity(childType);

                    childEntity.ServerPos.SetFrom(entity.ServerPos);
                    childEntity.ServerPos.Motion.X += (rand.NextDouble() - 0.5f) / 20f;
                    childEntity.ServerPos.Motion.Z += (rand.NextDouble() - 0.5f) / 20f;

                    childEntity.Pos.SetFrom(childEntity.ServerPos);
                    entity.World.SpawnEntity(childEntity);
                    entity.Attributes.SetString("origin", "reproduction");
                    childEntity.WatchedAttributes.SetInt("generation", generation + 1);
                }

            }

            entity.World.FrameProfiler.Mark("entity-multiply");
        }

        private bool TryGetPregnant()
        {
            if (entity.World.Rand.NextDouble() > 0.01) return false;
            if (!HasRequiredEntityNearby()) return false;
            if (TotalDaysCooldownUntil > entity.World.Calendar.TotalDays) return false;

            ITreeAttribute tree = entity.WatchedAttributes.GetTreeAttribute("hunger");
            if (tree == null) return false;

            float saturation = tree.GetFloat("saturation", 0);

            if (saturation >= PortionsEatenForMultiply)
            {
                if (entity.World.Rand.NextDouble() < 0.2)
                {
                    tree.SetFloat("saturation", saturation - 1);
                    return false;
                }

                tree.SetFloat("saturation", saturation - PortionsEatenForMultiply);
                IsPregnant = true;
                TotalDaysPregnancyStart = entity.World.Calendar.TotalDays;
                entity.WatchedAttributes.MarkPathDirty("multiply");

                return true;
            }

            return false;
        }


        float GetSaturation()
        {
            ITreeAttribute tree = entity.WatchedAttributes.GetTreeAttribute("hunger");
            if (tree == null) return 0;

            return tree.GetFloat("saturation", 0);
        }

        private bool HasRequiredEntityNearby()
        {
            if (RequiresNearbyEntityCode == null) return true;

            return entity.World.GetEntitiesAround(entity.ServerPos.XYZ, RequiresNearbyEntityRange, RequiresNearbyEntityRange, (e) =>
            {
                return e.WildCardMatch(new AssetLocation(RequiresNearbyEntityCode));
            }).Length > 0;
        }


        public override void OnEntityDespawn(EntityDespawnReason despawn)
        {
            entity.World.UnregisterCallback(callbackId);
        }



        public override string PropertyName()
        {
            return "multiply";
        }

        public override void GetInfoText(StringBuilder infotext)
        {
            multiplyTree = entity.WatchedAttributes.GetTreeAttribute("multiply");

            if (IsPregnant) infotext.AppendLine(Lang.Get("Is pregnant"));
            else
            {
                ITreeAttribute tree = entity.WatchedAttributes.GetTreeAttribute("hunger");
                if (tree != null)
                {
                    float saturation = tree.GetFloat("saturation", 0);
                    infotext.AppendLine(Lang.Get("Saturation: {0}", saturation));
                }
            }

            base.GetInfoText(infotext);
        }
    }
}