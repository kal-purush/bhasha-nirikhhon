using SuperRecall.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace SuperRecall.Helpers
{
    public class ElementsViewTemplateSelector : DataTemplateSelector
    {
        public DataTemplate QueueTemplate { get; set; }
        public DataTemplate RevisionTemplate { get; set; }

        public override DataTemplate SelectTemplate(object item, DependencyObject container)
        {
            return (item is QueueElement) ? QueueTemplate : RevisionTemplate;
        }
    }
}