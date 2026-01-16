using Microsoft.Xna.Framework;
using SeaBattle.Service.ShipSupplies;
using SeaBattle.View;

namespace SeaBattle.ShipSupplies
{
    public sealed class ClientWindVane : DrawableGameObject
    {
        public WindVane WindVane { get; set; }

        public override Vector2 MoveVector
        {
            get { return WindVane.Direction; }
        }

        public ClientWindVane()
        {
            WindVane = new WindVane(false);
            Coordinates = new Vector2(1400f, 50f);
            StaticTexture = Textures.CompassArrow;
        }
    }
}