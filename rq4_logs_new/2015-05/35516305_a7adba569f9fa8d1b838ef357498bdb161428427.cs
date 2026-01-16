using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeepSpaceBattles
{
    public class Boss
    {
        private Image boss;
        private List<BossWeapon> bweapons;
        public int life { get; set; }

        public int X { get; set; }
        public int Y { get; set; }

        private int speedX = 4;
        private int speedY = 2;

        public Boss(int X, int Y)
        {
            this.X = X;
            this.Y = Y;
            boss = DeepSpaceBattles.Properties.Resources.boss_2;
            bweapons = new List<BossWeapon>();
            life = 40;
        }

        public void BossMoving()
        {
            if (X > (1000 - boss.Size.Width - 80))
            {
                speedX = -speedX;
                X += speedX;
            }
            else if (X < 80)
            {
                speedX = -speedX;
                X += speedX;
            }
            else X += speedX;
            if (Y > 100)
            {
                speedY = -speedY;
                Y += speedY;
            }
            else if (Y < 20)
            {
                speedY = -speedY;
                Y += speedY;
            }
            else Y += speedY;
        }

        public bool GetsHit(Rectangle rec)
        {
            Rectangle newRec = new Rectangle(X, Y, boss.Width, boss.Height - 25);
            if (newRec.IntersectsWith(rec))
            {
                return true;
            }
            else return false;
        }

        public void Draw(Graphics g)
        {
            g.DrawImage(boss, X, Y, boss.Width, boss.Height);
        }

        public void FireWeaponLeft()
        {
            bweapons.Add(new BossWeapon(X + 70, Y + 135));
        }
        public void FireWeaponRight()
        {
            bweapons.Add(new BossWeapon(X + 220, Y + 135));
        }
        public void RemoveBossWeapon(int i)
        {
            bweapons.Remove(bweapons[i]);
        }
        public void MoveTheBossWeapon(int i)
        {
            bweapons[i].moveBossWeapon();
        }
        public List<BossWeapon> getBossWeapon()
        {
            return bweapons;
        }
    }
}