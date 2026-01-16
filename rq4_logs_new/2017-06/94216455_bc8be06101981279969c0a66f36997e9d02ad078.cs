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
using System.Runtime.InteropServices;
using System.Windows.Interop;
using System.Windows.Media.Animation;
using System.Threading;

namespace Portfolio
{
    /// <summary>
    /// Logique d'interaction pour MainWindow.xaml
    /// </summary>
    internal enum AccentState
    {
        ACCENT_DISABLED = 0,
        ACCENT_ENABLE_GRADIENT = 1,
        ACCENT_ENABLE_TRANSPARENTGRADIENT = 2,
        ACCENT_ENABLE_BLURBEHIND = 3,
        ACCENT_INVALID_STATE = 4
    }



    [StructLayout(LayoutKind.Sequential)]
    internal struct AccentPolicy
    {
        public AccentState AccentState;
        public int AccentFlags;
        public int GradientColor;
        public int AnimationId;
    }



    [StructLayout(LayoutKind.Sequential)]

    internal struct WindowCompositionAttributeData
    {
        public WindowCompositionAttribute Attribute;
        public IntPtr Data;
        public int SizeOfData;
    }



    internal enum WindowCompositionAttribute
    {
        // ...
        WCA_ACCENT_POLICY = 19
        // ...
    }

    public partial class MainWindow : Window
    {        
        [DllImport("user32.dll")]
        internal static extern int SetWindowCompositionAttribute(IntPtr hwnd, ref WindowCompositionAttributeData data);

        private bool _secondaryCvPaneOpened = false;

        public MainWindow()
        {
            InitializeComponent();
            SplashScreenVisual();
            
        }

        private void InitializeLineLengthInCV()
        {
            Line1.Y2 = ExSolutecCanvas.ActualHeight;
            Line2.Y2 = ExInfineonCanvas.ActualHeight;
            Line3.Y2 = ExCarsoCanvas.ActualHeight;
        }

        private void InitializeLineLengthInEducation()
        {
            Line4.Y2 = FoCPECanvas.ActualHeight;
            Line5.Y2 = FoPrepCPECanvas.ActualHeight;
        }

        private void SplashScreenVisual()
        {
            var animation = new DoubleAnimation
            {
                To = 0,
                BeginTime = TimeSpan.FromSeconds(4),
                Duration = TimeSpan.FromMilliseconds(500),
                FillBehavior = FillBehavior.Stop
            };

            animation.Completed += (s, a) => 
                {
                    SplashScreen.Visibility = System.Windows.Visibility.Hidden;
                    WelcomeText.Visibility = System.Windows.Visibility.Hidden;
                    ExplanationText.Visibility = System.Windows.Visibility.Hidden;
                    ExplorationText.Visibility = System.Windows.Visibility.Hidden;
                    InderminatePB.IsIndeterminate = false;
                    InderminatePB.Visibility = System.Windows.Visibility.Hidden;
                    this.ResizeMode = System.Windows.ResizeMode.CanResizeWithGrip;
                };

            SplashScreen.BeginAnimation(UIElement.OpacityProperty, animation);
        }

        private void Window_SizeChanged(object sender, SizeChangedEventArgs e)
        {
            InitializeLineLengthInCV();
            InitializeLineLengthInEducation();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            EnableBlur();
            InitializeLineLengthInCV();
            InitializeLineLengthInEducation();
        }

        internal void EnableBlur()
        {
            var windowHelper = new WindowInteropHelper(this);
            var accent = new AccentPolicy();
            accent.AccentState = AccentState.ACCENT_ENABLE_BLURBEHIND;

            var accentStructSize = Marshal.SizeOf(accent);
            var accentPtr = Marshal.AllocHGlobal(accentStructSize);

            Marshal.StructureToPtr(accent, accentPtr, false);

            var data = new WindowCompositionAttributeData();
            data.Attribute = WindowCompositionAttribute.WCA_ACCENT_POLICY;
            data.SizeOfData = accentStructSize;
            data.Data = accentPtr;

            SetWindowCompositionAttribute(windowHelper.Handle, ref data);
            Marshal.FreeHGlobal(accentPtr);
        }

        /// <summary>
        /// TitleBar_MouseDown - Drag if single-click, resize if double-click
        /// </summary>
        private void TitleBar_MouseDown(object sender, MouseButtonEventArgs e)
        {
            if (e.ChangedButton == MouseButton.Left)
                Application.Current.MainWindow.DragMove();
        }

        /// <summary>
        /// CloseButton_Clicked
        /// </summary>
        private void CloseButton_Click(object sender, RoutedEventArgs e)
        {
            Application.Current.Shutdown();
        }

        /// <summary>
        /// Minimized Button_Clicked
        /// </summary>
        private void MinimizeButton_Click(object sender, RoutedEventArgs e)
        {
            this.WindowState = WindowState.Minimized;
        }

        /// <summary>
        /// Minimized Button_Clicked
        /// </summary>
        private void MaximizeButton_Click(object sender, RoutedEventArgs e)
        {
            if (this.WindowState == System.Windows.WindowState.Normal)
                this.WindowState = WindowState.Maximized;
            else if (this.WindowState == System.Windows.WindowState.Maximized)
                this.WindowState = System.Windows.WindowState.Normal;
        }

        private void TabControl_SelectionChanged(object sender, RoutedEventArgs e)
        {
            if (TabItemCV != null && TabItemCV.IsSelected)
            {
                CvScrollViewer.ScrollToTop();
                ChangeProgressBarValueOnLoad(CppProgressbar, 75);
                ChangeProgressBarValueOnLoad(CProgressbar, 75);
                ChangeProgressBarValueOnLoad(CsProgressbar, 50);
                ChangeProgressBarValueOnLoad(MatlabProgressbar, 50);
                ChangeProgressBarValueOnLoad(PerlProgressbar, 40);
                ChangeProgressBarValueOnLoad(JavaProgressbar, 30);
                ChangeProgressBarValueOnLoad(VbaProgressbar, 25);
            }
            if (TabItemProjects != null && TabItemProjects.IsSelected)
            {
                ChangeProgressBarValueOnUnload(CppProgressbar, 0);
                ChangeProgressBarValueOnUnload(CProgressbar, 0);
                ChangeProgressBarValueOnUnload(CsProgressbar, 0);
                ChangeProgressBarValueOnUnload(MatlabProgressbar, 0);
                ChangeProgressBarValueOnUnload(PerlProgressbar, 0);
                ChangeProgressBarValueOnUnload(JavaProgressbar, 0);
                ChangeProgressBarValueOnUnload(VbaProgressbar, 0);
                _secondaryCvPaneOpened = CloseOpenedPanes(_secondaryCvPaneOpened, CVSecondaryNavPane, OpenCvSecondaryPaneButtonIcon, SecondaryNavPaneStackPanel);
            }
        }

        private void ChangeProgressBarValueOnLoad(ProgressBar bar, int value)
        {
            Duration duration = new Duration(TimeSpan.FromSeconds(1));
            DoubleAnimation doubleanimation = new DoubleAnimation(value, duration);
            bar.BeginAnimation(ProgressBar.ValueProperty, doubleanimation);
        }

        private void ChangeProgressBarValueOnUnload(ProgressBar bar, int value)
        {
            Duration duration = new Duration(TimeSpan.FromMilliseconds(1));
            DoubleAnimation doubleanimation = new DoubleAnimation(value, duration);
            bar.BeginAnimation(ProgressBar.ValueProperty, doubleanimation);
        }

        private bool CloseOpenedPanes(bool paneOpened, Rectangle SecondaryNavPaneRectangle, Image ButtonIcon, StackPanel panel)
        {
            if (paneOpened)
            {
                SecondaryNavPaneColumn.Width = new GridLength(0);
                RootGridSecondaryPaneCol.Width = new GridLength(0);

                Duration duration = new Duration(TimeSpan.FromMilliseconds(100));
                DoubleAnimation doubleanimation = new DoubleAnimation(0, duration);
                DoubleAnimation windowAnimation = new DoubleAnimation(this.Width - 185, duration);

                SecondaryNavPaneRectangle.BeginAnimation(Rectangle.WidthProperty, doubleanimation);
                this.BeginAnimation(MainWindow.WidthProperty, windowAnimation);
                AppBarSecondaryNavPane.BeginAnimation(Rectangle.WidthProperty, doubleanimation);
                panel.BeginAnimation(StackPanel.WidthProperty, doubleanimation);

                ButtonIcon.Source = new BitmapImage(new Uri(@"icon/icons8-Forward.png", UriKind.Relative));

                paneOpened = false;
            }
            return paneOpened;
        }

        private void OpenSecondaryCVNavPaneButton_Click(object sender, RoutedEventArgs e)
        {
            if (TabItemCV.IsSelected)
            {
                if (!_secondaryCvPaneOpened)
                {
                    SecondaryNavPaneColumn.Width = new GridLength(185);
                    RootGridSecondaryPaneCol.Width = new GridLength(185);

                    Duration duration = new Duration(TimeSpan.FromMilliseconds(100));
                    DoubleAnimation doubleanimation = new DoubleAnimation(185, duration);
                    DoubleAnimation windowAnimation = new DoubleAnimation(this.Width+185,duration);

                    CVSecondaryNavPane.BeginAnimation(Rectangle.WidthProperty, doubleanimation);
                    this.BeginAnimation(MainWindow.WidthProperty, windowAnimation);
                    AppBarSecondaryNavPane.BeginAnimation(Rectangle.WidthProperty, doubleanimation);
                    SecondaryNavPaneStackPanel.BeginAnimation(StackPanel.WidthProperty, doubleanimation);

                    OpenCvSecondaryPaneButtonIcon.Source = new BitmapImage(new Uri(@"icon/icons8-Back.png", UriKind.Relative));

                    CvScrollViewer.ScrollToTop();

                    _secondaryCvPaneOpened = true;

                }
                else
                {
                    SecondaryNavPaneColumn.Width = new GridLength(0);
                    RootGridSecondaryPaneCol.Width = new GridLength(0);

                    Duration duration = new Duration(TimeSpan.FromMilliseconds(100));
                    DoubleAnimation doubleanimation = new DoubleAnimation(0, duration);
                    DoubleAnimation windowAnimation = new DoubleAnimation(this.Width - 185, duration);

                    CVSecondaryNavPane.BeginAnimation(Rectangle.WidthProperty, doubleanimation);
                    this.BeginAnimation(MainWindow.WidthProperty, windowAnimation);
                    AppBarSecondaryNavPane.BeginAnimation(Rectangle.WidthProperty, doubleanimation);
                    SecondaryNavPaneStackPanel.BeginAnimation(StackPanel.WidthProperty, doubleanimation);

                    OpenCvSecondaryPaneButtonIcon.Source = new BitmapImage(new Uri(@"icon/icons8-Forward.png", UriKind.Relative));

                    _secondaryCvPaneOpened = false;
                }
            }
        }

    }
}