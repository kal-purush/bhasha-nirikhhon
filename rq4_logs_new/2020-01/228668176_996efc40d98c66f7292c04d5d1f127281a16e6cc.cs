/*
Copyright © 2017-2019 César Andrés Morgan
Licenciado para uso interno solamente.
*/

using System;
using TheXDS.Proteus.Crud.Base;
using TheXDS.Proteus.Crud.Mappings.Base;
using Xceed.Wpf.Toolkit;

namespace TheXDS.Proteus.Crud.Mappings
{
    public class DateTimeUpDownRangeMapping : XceedUpDownRangeMapping<DateTimePicker, DateTime>
    {
        public DateTimeUpDownRangeMapping(IPropertyDescription property) : base(property)
        {
            if (property is IPropertyDateDescription d)
            {
                _lower.Format = d.WithTime ? DateTimeFormat.FullDateTime : DateTimeFormat.LongDate;
                _upper.Format = d.WithTime ? DateTimeFormat.FullDateTime : DateTimeFormat.LongDate;
            }
        }
    }
}