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

namespace OsuSqlTool.Controls
{
    /// <summary>
    /// Interaktionslogik für RoundButton.xaml
    /// </summary>
    public partial class RoundButton : Button
    {
        public RoundButton()
        {
            InitializeComponent();
        }

        public Color HoverColor
        {
            get { return (Color)GetValue(HoverColorProperty); }
            set { SetValue(HoverColorProperty, value); }
        }

        // Using a DependencyProperty as the backing store for HoverColor.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty HoverColorProperty =
            DependencyProperty.Register("HoverColor", typeof(Color), typeof(RoundButton), new PropertyMetadata(Colors.LightCyan));


    }
}