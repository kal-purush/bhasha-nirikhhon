using Syncfusion.UI.Xaml.TreeGrid;
using System;
using System.Collections.Generic;
using System.Globalization;
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

namespace SfTreeGridDemo
{

    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }
    }

    public class ToolTipConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var treeGridcell = value as TreeGridCell;

            string cellvalue = string.Empty;
            var column = treeGridcell.ColumnBase.TreeGridColumn;
            var treeGrid = column.GetType().GetProperty("TreeGrid", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).GetValue(column) as SfTreeGrid;

            if (treeGridcell != null && treeGridcell.ColumnBase != null && !treeGridcell.ColumnBase.IsEditing)
            {
                string formattedValue = "";
                object value1 = null;
                if (treeGridcell.ColumnBase.ColumnElement != null)
                { value1 = treeGridcell.ColumnBase.ColumnElement.DataContext; }
                if (treeGridcell.ColumnBase.TreeGridColumn != null && treeGrid != null && treeGrid.View != null && treeGridcell.ColumnBase.TreeGridColumn.MappingName != null)
                {
                    var tempFormatValue = treeGrid.View.GetPropertyAccessProvider().GetFormattedValue(value1, treeGridcell.ColumnBase.TreeGridColumn.MappingName);
                    formattedValue = tempFormatValue == null ? null : tempFormatValue.ToString();
                    cellvalue = formattedValue;
                }
            }
            return cellvalue;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}