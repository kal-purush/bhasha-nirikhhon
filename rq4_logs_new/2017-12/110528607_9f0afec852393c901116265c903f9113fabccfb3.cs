using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace CRM_GTMK.Model
{
	public class XmlHelper
	{
		private const string TEST_BASE_DIRECTORY = "..\\..\\Model\\XMLBase";
		private const string BASE_DIRECTORY = "Model\\XMLBase";
		private const string COMPANY_FILE_NAME = "companies.xml";
		public void AddNewCompanyInfo()
		{

			string path = "";
			XDocument xDoc = new XDocument();
			try
			{
				path = Path.Combine(TEST_BASE_DIRECTORY, COMPANY_FILE_NAME);
				xDoc = XDocument.Load(path);
			}
			catch (FileNotFoundException)
			{
				path = Path.Combine(BASE_DIRECTORY, COMPANY_FILE_NAME);
				xDoc = XDocument.Load(path);
			}
			
			xDoc.Element("companies").Add(new XElement("company"));

			xDoc.Save(path);
		}
	}
}