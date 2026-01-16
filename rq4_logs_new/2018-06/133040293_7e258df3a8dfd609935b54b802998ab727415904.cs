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
using System.Drawing;

namespace U4DonkeyKong
{
    class Barrel
    {
        DispatcherTimer GameTimer = new DispatcherTimer();
        Window window;
        Canvas canvas;
        int y_velocity;
        Point point = new Point(0, 0);
        public Barrel(Window w, Canvas c)
        {
            window = w;
            canvas = c;
        }
        public void update(Ellipse barrel)
        {
            Canvas.SetLeft(barrel, point.X);
            Canvas.SetTop(barrel, point.Y);
        }
        
        public void generate(Ellipse barrel)
        {
            barrel.Height = 50;
            barrel.Width = 50;
            barrel.Fill = Brushes.Aqua;
            Canvas.SetLeft(barrel, 50);
            Canvas.SetTop(barrel, 50);
        }
    }
}