namespace TowerDefense.Main.Monsters
{
    using TowerDefense.Interfaces;
    using TowerDefense.Utils;

    public class Dragon : Monster
    {
        protected override int SpriteColumns
        {
            get
            {
                return 4;
            }
        }

        public Dragon(IRoute route) :
            base(route, 50, 500, 10, 200, ImageFactory.CreateImage("dragon.png"))
        {
        }
    }
}