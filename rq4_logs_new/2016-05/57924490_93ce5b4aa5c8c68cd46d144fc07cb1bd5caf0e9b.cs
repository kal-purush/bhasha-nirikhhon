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

namespace KeyboardShortcutDetector.Demo
{
    public class LeftCtrlDigitShortcut : RangeShortcut
    {
        public LeftCtrlDigitShortcut() : base(
            new []{Key.LeftCtrl }, 
            KeyRange.Digits())
        {
        }
    }

    public class RightCtrlLetterShortcut : RangeShortcut
    {
        public RightCtrlLetterShortcut() : base(
            new []{Key.RightCtrl},
            KeyRange.Letters())
        {
        }
    }

    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window, 
        IShortcutObserver<LeftCtrlDigitShortcut>,
        IShortcutObserver<RightCtrlLetterShortcut>
    {
        private IKeyboardShortuctDetector _detector;

        public MainWindow()
        {
            InitializeComponent();
            InitializeShortcutDetector();
        }

        private void InitializeShortcutDetector()
        {
            _detector = new KeyboardShortcutDetectorFactory().Create();
            _detector.RegisterShortcut(new LeftCtrlDigitShortcut());
            _detector.RegisterShortcut(new RightCtrlLetterShortcut());

            _detector.Subscribe(this);
        }

        public void ShortcutPressed(LeftCtrlDigitShortcut shortcut)
        {
            Result.Text = "LeftCtr + " + shortcut.LastKeyInRange.ToString();
        }

        public void ShortcutReleased(LeftCtrlDigitShortcut shortcut)
        {
            Result.Text = "LeftCtrl combination released";
        }

        public void ShortcutPressed(RightCtrlLetterShortcut shortcut)
        {
            Result.Text = "RightCtrl + " + shortcut.LastKeyInRange.ToString();
        }

        public void ShortcutReleased(RightCtrlLetterShortcut shortcut)
        {
            Result.Text = "RightCtrl combination released";
        }
    }
}