using System.Collections.Generic;
using SeaBattle.Common.Objects;
using SeaBattle.Service.Ships;

namespace SeaBattle.Service
{
    internal class CollizionDetector
    {
        private static CollizionDetector _collizionDetector;
        public static CollizionDetector Instance
        {
            get
            {
                if (_collizionDetector != null) return _collizionDetector;

                _collizionDetector = new CollizionDetector();;
                return _collizionDetector;
            }
        }

        public void CollizionForBulletAndShip(IBullet bullet, ShipBase ship)
        {
            if (((bullet.Coordinates - ship.Coordinates).Length() > 32) || (bullet.Shooter == ship.Player.Name)) return;

            var firstPart = ship.Coordinates + ship.MoveVector*15;
            var secondPart = ship.Coordinates - ship.MoveVector * 15;
            if (((bullet.Coordinates - ship.Coordinates).Length() > 16) && ((bullet.Coordinates - firstPart).Length() > 16) && ((bullet.Coordinates - secondPart).Length() > 16)) return;

            ship.Health -= bullet.Damage;
            bullet.Dispose();
        }

        public void UpdateObjects(List<IBullet> bullets, List<ShipBase> ships)
        {
            foreach (var ship in ships)
            {
                foreach (var bullet in bullets)
                {
                    CollizionForBulletAndShip(bullet, ship);
                }   
            }
        }


    }
}