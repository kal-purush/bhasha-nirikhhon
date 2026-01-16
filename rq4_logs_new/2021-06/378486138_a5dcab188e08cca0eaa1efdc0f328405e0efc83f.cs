using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Drawing;
namespace TankGameV._10版本
{
    class Bullet:GameObject
    {
        private Image img;

        public Image Img
        {
            get { return img; }
            set { img = value; }
        }

        public int Power
        {
            get;set;
        }

        //子弹的边长为12
        public Bullet(Tank tank, int speed, int life, int power, Image img)
            : base(tank.X + tank.Width / 2 - 6, tank.Y + tank.Height / 2 - 6, img.Width, img.Height,speed,life,tank.Dir)
        {
            this.img = img;
        }

        public override void Draw(Graphics g)
        {
            switch (this.Dir)
            {
                case Direction.Up:
                    this.Y -= this.Speed;
                    break;
                case Direction.Down:
                    this.Y += this.Speed;
                    break;
                case Direction.Left:
                    this.X -= this.Speed;
                    break;
                case Direction.Right:
                    this.X += this.Speed;
                    break;
            }
            //在游戏对象移动完成后 我们应该判断一下 当前游戏对象是否超出当前的窗体 
            /*if (this.X <= 0)
            {
                this.X = -100;
            }
            if (this.Y <= 0)
            {
                this.Y = -100;
            }
            if (this.X >= 800)
            {
                this.X = 900;
            }
            if (this.Y >= 700)
            {
                this.Y = 800;
            }*/   if (this.X < 0 || this.X > 800 || this.Y < 0 || this.Y > 700) 
                GameController.GetInstance().RemoveGameObject(this);
            g.DrawImage(img, this.X, this.Y);
         
        }



    }
}