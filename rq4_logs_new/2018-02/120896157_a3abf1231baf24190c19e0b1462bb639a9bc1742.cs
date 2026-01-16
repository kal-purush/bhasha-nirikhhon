using Infragistics.Windows.Editors;
using System;
using System.Windows;
using System.Windows.Controls;

namespace HighFreqUpdate.Selector
{
    public class DecimalFormatTemplateSelectorNew : DataTemplateSelector
    {
        #region Members
        public override DataTemplate SelectTemplate(object item, DependencyObject container)
        {
            try
            {
                DataTemplate dataTemplate = null;

                var editor = TemplateEditor.GetEditor(container);

                // clearing the Tag property which in this case is used
                // to track the template returned in edit mode.
                editor.Tag = null;

                var dataContext = ((FrameworkElement)((ContentPresenter)container).Parent).DataContext;

                if (dataContext != null)
                {
                    var dI = ((Infragistics.Windows.DataPresenter.DataRecord)((FrameworkElement)((ContentPresenter)container).Parent).DataContext).DataItem;

                    if (dI is Models.DealVisualBase dataItem)
                    {
                        if (item != null)
                        {
                            switch (dataItem.IdCross)
                            {
                                case 1:
                                    dataTemplate = editor.FindResource("ctvFormatDecimal2Qta1") as DataTemplate;
                                    break;
                                case 2:
                                    dataTemplate = editor.FindResource("ctvFormatDecimal4Qta1") as DataTemplate;
                                    break;
                            }
                        }

                        if (dataTemplate != null)
                        {
                            return dataTemplate;
                        }
                    }
                }

                return base.SelectTemplate(item, container);
            }
            catch (Exception ex)
            {
                return base.SelectTemplate(item, container);
            }
        }
        #endregion
    }
}