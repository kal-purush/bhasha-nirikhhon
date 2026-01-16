using System;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class PortalSys : ISystem<float>
    {
        EntitySet portalSet;

        public PortalSys()
        {
            portalSet = Game1.world.GetEntities().With<CGridPosition>().With<CPortal>().AsSet();
        }

        public bool IsEnabled { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void Dispose() { }

        public void Update(float state)
        {
            var entities = portalSet.GetEntities();

            var pPos = Game1.Player.Get<CGridPosition>();
            var wepArray = Game1.Player.Get<CWeaponsArray>();

            foreach (var e in entities)
            {
                var portalPos = e.Get<CGridPosition>();
                int dX = pPos.X - portalPos.X;
                int dY = pPos.Y - portalPos.Y;
                if (dX >= -1 && dX <= 1 && dY >= -1 && dY <= 1)
                {
                    Game1.GameState = GameState.Starting;
                    Game1.startingWeapons = new CWeaponsArray(wepArray);
                    return;
                }
            }
        }
    }
}