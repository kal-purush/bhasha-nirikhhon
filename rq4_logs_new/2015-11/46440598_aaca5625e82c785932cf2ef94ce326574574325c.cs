using System;
using System.Xml;
using System.Xml.Linq;

namespace IOXMLFiles
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			string value = "simple value", property = "simple property";

			string path = System.IO.Path.Combine (AppDomain.CurrentDomain.BaseDirectory, "test.xml");

			using (XmlWriter writer = XmlWriter.Create (path)) {
			
				writer.WriteStartDocument ();

				writer.WriteStartElement ("settings");


				//Съдържание на файла
				writer.WriteStartElement ("row");

				writer.WriteAttributeString ("prorerty", property); //<row property="..."> ...</row>
				writer.WriteString (value); //<row> value </row> 

				writer.WriteEndElement ();

				writer.WriteEndElement ();

				writer.WriteEndDocument ();
			
			}

			XDocument document = XDocument.Load (path);

			document.Save (path); 


			System.Diagnostics.Process.Start (path);
		}
	}
}