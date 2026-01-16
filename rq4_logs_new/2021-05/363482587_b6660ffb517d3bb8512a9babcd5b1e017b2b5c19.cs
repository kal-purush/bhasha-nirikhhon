using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OSIsoft.DataViews;

namespace DataViews
{
    public static class FieldExtension
    {
        public static Field Clone(this Field field)
        {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            var newField = new Field
            {
                Source = field.Source,
                IncludeUom = field.IncludeUom,
                Label = field.Label,
                SummaryDirection = field.SummaryDirection,
                SummaryType = field.SummaryType,
            };
            ((List<string>)newField.Keys).AddRange(field.Keys);
            ((List<string>)newField.StreamReferenceNames).AddRange(field.StreamReferenceNames);

            return newField;
        }
    }
}