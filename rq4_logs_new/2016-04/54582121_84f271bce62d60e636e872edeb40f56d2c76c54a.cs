using System;
using System.Data;
using System.Xml;
using System.Collections.Generic;
using RootNav.Core;
using System.IO;
using RootNav.Core.MixtureModels; 

namespace RootNavLinux
{
	public class OutputResultXML
	{
		public static string FullOutputFileName{ get; set; }

		public OutputResultXML ()
		{
		}

		public static void writeInputData(string originFilename, string inputPath, string outputPath, EMConfiguration config)
		{
			//System.IO.Stream s = new FileStream(FullOutputFileName, FileMode.OpenOrCreate);
			//System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer();

			using (XmlWriter writer = XmlWriter.Create(FullOutputFileName))
			{
				writer.WriteStartDocument();
				writer.WriteStartElement("DataProcessed");


				writer.WriteStartElement("Input");

				writer.WriteStartElement("File");
				writer.WriteElementString("ImageFile", originFilename);
				writer.WriteElementString("InputPath", inputPath);
				writer.WriteElementString("OutputPath", outputPath);
				writer.WriteEndElement(); //end File

				writer.WriteStartElement("EMConfiguration");
				writer.WriteElementString("Name", config.Name);
				writer.WriteElementString("InitialClassCount", config.InitialClassCount.ToString());
				writer.WriteElementString("MaximumClassCount", config.MaximumClassCount.ToString());
				writer.WriteElementString("ExpectedRootClassCount", config.ExpectedRootClassCount.ToString());
				writer.WriteElementString("PatchSize", config.PatchSize.ToString());
				writer.WriteElementString("BackgroundPercentage", config.BackgroundPercentage.ToString());
				writer.WriteElementString("BackgroundExcessSigma", config.BackgroundExcessSigma.ToString());
				writer.WriteStartElement("Weights");
				foreach (double w in config.Weights) {
					writer.WriteElementString("double", w.ToString());	
				}
				writer.WriteEndElement(); //end Weights

				writer.WriteEndElement(); //end Configuration

				writer.WriteEndElement(); //end Input

				writer.WriteEndElement(); //end DataProcessed
				writer.WriteEndDocument();
			}

		}

		public static void writeTipsDetected(List<Int32Point> points, bool isAppended)
		{
			//System.IO.Stream s = new FileStream(FullOutputFileName, FileMode.OpenOrCreate);
			//System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer();
		}
	}
}
