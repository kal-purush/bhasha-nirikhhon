using System.Windows;
using System.Windows.Controls;

namespace GoShopping
{
    public class SettingsTemplateSelector : DataTemplateSelector
    {
        public DataTemplate CheckBoxTemplate { get; set; }
        public DataTemplate TextBoxTemplate { get; set; }

        public override DataTemplate SelectTemplate(object item, DependencyObject container)
        {
            MainWindow.Setting setting = item as MainWindow.Setting;
            if (setting.IsCheckBox)
            {
                return CheckBoxTemplate;
            }
            return TextBoxTemplate;
        }
    }
}