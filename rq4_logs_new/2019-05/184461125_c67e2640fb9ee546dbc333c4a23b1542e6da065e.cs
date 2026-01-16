using System;
using System.Collections.Generic;
using System.Text;

namespace Zad_1_POPRAWA
{
	class DataFiller
	{
		public void Fill(ref DataContext dataContext)
		{
			var gamblers = dataContext.gamblers;
			var machines = dataContext.machines;
			var events = dataContext.events;

			Gambler gambler1 = new Gambler("Konrad", "Plawik", 123456, true);

			dataContext.gamblers.Add(gambler1);
		}
	}
}