using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.Windows.Threading;

namespace Donkey_Kong
{
    class Player
    {
        int y_vel = 0;
        Window window;
        Canvas canvas;
        bool playerisgenerated = false;

        Point point = new Point(200,200);

        public Player(Window w, Canvas c)
        {
            window = w;
            canvas = c;
        }
        public void move()
        {
            if (point.X > 25 && point.X < 730)
            {
                Console.WriteLine(point.X + ", " + point.Y);
                if (point.Y > 525)
                {
                    if (Keyboard.IsKeyDown(Key.Left))
                    {
                        point.X -= 5;
                    }
                    if (Keyboard.IsKeyDown(Key.Right))
                    {
                        point.X += 5;
                    }
                }
                if ((point.Y < 500 && point.Y > 380) || (point.Y < 240 && point.Y > 165))
                {
                    if (Keyboard.IsKeyDown(Key.Left))
                    {
                        point.X -= 4.75;
                        point.Y -= 0.25;
                    }
                    if (Keyboard.IsKeyDown(Key.Right))
                    {
                        point.X += 4.75;
                        point.Y += 0.25;
                    }
                }
                if ((point.Y < 380 && point.Y > 325) || (point.Y < 165 && point.Y > 125))
                {
                    if (Keyboard.IsKeyDown(Key.Left))
                    {
                        point.X -= 4.75;
                        point.Y += 0.25;
                    }
                    if (Keyboard.IsKeyDown(Key.Right))
                    {
                        point.X += 4.75;
                        point.Y -= 0.25;
                    }
                }
                
            }
            
           
        }
        public void fall()
        {
            if (point.Y == 590)
            {
                y_vel = 0;
            }
            else if (point.Y > 590)
            {
                point.Y = 640;
                y_vel = 0;
            }
            else if ((point.X > 635) || (point.Y > 230 && point.Y < 270))
            {
                point.Y += 3;
                point.X += 0.25;
            }
        }
        public void jump()
        {
            //Console.WriteLine("is diplasy");
            if (Keyboard.IsKeyDown(Key.Up))
            {
                for (int i = 0; i <= 5; i++)
                {

                }
            }
        }
        public void climb()
        {

        }
        public void collide()
        {

        }
        public void update(Rectangle player_sprite)
        {
            Canvas.SetLeft(player_sprite, point.X);
            Canvas.SetTop(player_sprite, point.Y);
        }
        public void generate(Rectangle player_sprite)
        {
            
            
            player_sprite.Width = 25;
            player_sprite.Height = 50;
            player_sprite.Fill = Brushes.White;
            Canvas.SetLeft(player_sprite, point.X);
            Canvas.SetTop(player_sprite, point.Y);
            


            
        }
    }
}