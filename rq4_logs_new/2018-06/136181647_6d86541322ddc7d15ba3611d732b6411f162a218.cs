using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using System.Collections;

namespace u5_Culminating
{
    class Bomb
    {
        //Generate Player Variables
        Point BombPos = new Point();
        private Point point;
        public Point Point { get => point; }
        Canvas canvas;
        MainWindow window;
        Rectangle BombRectangle;
        public Rect boundingBox { get => box; }
        Rect box;
        Random r = new Random(5);
        Random x_random = new Random();
        int Velocity = -40 - Util.FruitVelocity();
        string movement;

        public Bomb(Canvas c, MainWindow w)
        {
            //Generate Alien
            canvas = c;
            window = w;

            ImageBrush s_Bomb = new ImageBrush(new BitmapImage(new Uri(@"Images\Bomb.png", UriKind.Relative)));

            point.Y = 690;
            point.X = 150 + x_random.Next(0, 251);
            if (point.X < 300)
            {
                movement = "left";
            }
            else if (point.X > 300)
            {
                movement = "right";
            }
            BombPos = point;
            BombRectangle = new Rectangle();
            BombRectangle.Fill = s_Bomb;
            BombRectangle.Height = 64;
            BombRectangle.Width = 64;
            canvas.Children.Add(BombRectangle);
            box = new Rect(point, new Size(64, 64));
            int rOthernumber = r.Next();



        }




        public void Tick()
        {
            Movement();
            Velocity = Velocity + 2;

            Canvas.SetTop(BombRectangle, point.Y);
            Canvas.SetLeft(BombRectangle, point.X);
            box.X = point.X;
            box.Y = point.Y;
        }





        private void Movement()
        {
            point.Y = point.Y + (Velocity);
            if (movement == "left")
            {
                point.X = point.X + 5;
            }
            else if (movement == "right")
            {
                point.X = point.X - 5;
            }

        }


        public bool collidesWith(Sword sword)
        {
            if (this.boundingBox.X > (sword.boundingBox.X - 32) && this.boundingBox.X < (sword.boundingBox.X + 80)
                && this.boundingBox.Y < (sword.boundingBox.Y + 32) && this.boundingBox.Y > sword.boundingBox.Y - 32)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public void destroy()
        {
            canvas.Children.Remove(BombRectangle);


        }

    }

}