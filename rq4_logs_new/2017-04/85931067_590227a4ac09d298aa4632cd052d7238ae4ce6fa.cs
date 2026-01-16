using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fynbus
{
	class Route
	{
		public static List<Route> Routes = new List<Route>();
		public int RouteID;
		public List<Offer> Offers;

		public Route(int routeid)
		{
			RouteID = routeid;
			Offers = new List<Offer>();
		}
	}
}