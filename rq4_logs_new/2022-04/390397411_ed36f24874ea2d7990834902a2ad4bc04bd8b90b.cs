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

namespace Wisdom.Controls
{
    /// <summary>
    /// Логика взаимодействия для MultiComboBox.xaml
    /// </summary>
    public partial class MultiComboBox : UserControl
    {
        public MultiComboBox()
        {
            InitializeComponent();
        }

        public static readonly DependencyProperty ItemsSourceProperty =
            DependencyProperty.Register("ItemsSource", typeof(Dictionary<string, object>),
            typeof(MultiComboBox), new UIPropertyMetadata(string.Empty));

        public static readonly DependencyProperty SelectedItemsProperty =
           DependencyProperty.Register("SelectedItems",
           typeof(Dictionary<string, object>),
           typeof(MultiComboBox), new UIPropertyMetadata(string.Empty));
        public static readonly DependencyProperty TextProperty =
           DependencyProperty.Register("Text",
           typeof(string), typeof(MultiComboBox),
           new UIPropertyMetadata(string.Empty));

        public static readonly DependencyProperty DefaultTextProperty =
            DependencyProperty.Register("DefaultText", typeof(string),
            typeof(MultiComboBox), new UIPropertyMetadata(string.Empty));

        public Dictionary<string, object> ItemsSource
        {
            get
            {
                return (Dictionary<string, object>)
                    GetValue(ItemsSourceProperty);
            }
            set
            {
                SetValue(ItemsSourceProperty, value);
            }
        }

        public Dictionary<string, object> SelectedItems
        {
            get
            {
                return (Dictionary<string, object>)
                    GetValue(SelectedItemsProperty);
            }
            set
            {
                SetValue(SelectedItemsProperty, value);
            }
        }

        public string Text
        {
            get { return (string)GetValue(TextProperty); }
            set { SetValue(TextProperty, value); }
        }

        public string DefaultText
        {
            get { return (string)GetValue(DefaultTextProperty); }
            set { SetValue(DefaultTextProperty, value); }
        }
    }
}