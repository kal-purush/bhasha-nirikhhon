using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Media;
using System.ComponentModel;
using System.Windows;

namespace BouncingBallSample
{
    public partial class Brick: INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged(string propertyName)
        {
            if (PropertyChanged != null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
            }
        }

        private double _brickCanvasTop;
        public double BrickCanvasTop
        {
            get { return _brickCanvasTop; }
            set
            {
                _brickCanvasTop = value;
                OnPropertyChanged("BrickCanvasTop");
            }
        }

        private double _brickCanvasLeft;
        public double BrickCanvasLeft
        {
            get { return _brickCanvasLeft; }
            set
            {
                _brickCanvasLeft = value;
                OnPropertyChanged("BrickCanvasLeft");
            }
        }

        private double _brickHeight;
        public double BrickHeight
        {
            get { return _brickHeight; }
            set
            {
                _brickHeight = value;
                OnPropertyChanged("BrickHeight");
            }
        }

        private double _brickWidth;
        public double BrickWidth
        {
            get { return _brickWidth; }
            set
            {
                _brickWidth = value;
                OnPropertyChanged("BrickWidth");
            }
        }

        private Visibility _brickVisibility;
        public Visibility BrickVisibility
        {
            get { return _brickVisibility; }
            set
            {
                _brickVisibility = value;
                OnPropertyChanged("BrickVisibility");
            }
        }

        private Brush _fill;
        public Brush Fill
        {
            get { return _fill; }
            set
            {
                _fill = value;
                OnPropertyChanged("Fill");
            }
        }
    }
}