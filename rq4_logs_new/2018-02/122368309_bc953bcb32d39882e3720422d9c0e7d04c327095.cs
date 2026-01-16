using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParseTests
{
	[TestClass]
	public class FileTests
	{

		[TestMethod]
		public void AddNewVoc()
		{
			string[] args = new[] { "add", "de:Küste", "en:coast", "hint" };
			Parser p = new Parser();
			Action<IRepository> a = p.Parse(args);
			FileRepository repository = new FileRepository();
			a(repository);

			Assert.AreEqual("coast", repository.Items["de:Küste"].Words["en"]);
			Assert.AreEqual("Küste", repository.Items["en:coast"].Words["de"]);
		}
	}
}