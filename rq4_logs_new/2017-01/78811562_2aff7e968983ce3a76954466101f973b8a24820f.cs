using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Media;

namespace NewellClark.Wpf.UserControls.TypeConverters
{
	public class BooleanToBrushConverter : BooleanToToggleConverter<Brush>
	{
		public BooleanToBrushConverter()
			: base(new SolidColorBrush(Colors.Black), new SolidColorBrush(Colors.Red)) { }
		public BooleanToBrushConverter(SolidColorBrush @true, SolidColorBrush @false)
			: base(@true, @false) { }
	}
}