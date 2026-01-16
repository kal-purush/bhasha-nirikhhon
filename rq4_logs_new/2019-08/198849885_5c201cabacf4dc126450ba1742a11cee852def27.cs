using Autodesk.Revit.DB;
using Autodesk.Revit.UI;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

namespace DockablePane
{
    /// <summary>
    /// Interaction logic for MellowWindow.xaml
    /// </summary>
    public partial class MellowWindow : UserControl
    {
        //public ObservableCollection<StairViewMappingTest> stairViewMappings { get; set; }
        public ObservableCollection<StairViewMapping> stairViewMappings { get; set; }
        public UIApplication application { get; set; }

        public MellowWindow(UIApplication _application, ObservableCollection<StairViewMapping> _stairViewMappings)
        {
            InitializeComponent();

            this.stairViewMappings = _stairViewMappings;
            this.application = _application;

            this.DataContext = this;
        }

        private void OpenView(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            TextBlock item = sender as TextBlock;
            if (item != null)
            {
                String stairIdString = item.Tag.ToString().Split(null)[1];
                String viewIdString = item.Tag.ToString().Split(null)[0];

                UIDocument document = application.ActiveUIDocument;

                document.Selection.SetElementIds(new List<ElementId>() { new ElementId(int.Parse(stairIdString)) });
                document.RequestViewChange(document.Document.GetElement(new ElementId(int.Parse(viewIdString))) as View);
            }
        }
    }

    // actual class to encapsulate data from DockablePane
    public class StairViewMapping
    {
        public String stairId { get; set; }

        public ObservableCollection<ViewDetails> viewIds { get; set; }

        public override string ToString()
        {
            return stairId.ToString();
        }
    }

    public class ViewDetails
    {
        public String viewId { get; set; }
        public String stairId { get; set; }
    }

    public class NameMultiValueConverter : IMultiValueConverter
    {
        public object Convert(object[] values, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            return String.Format("{0} {1}", values[0], values[1]);
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}