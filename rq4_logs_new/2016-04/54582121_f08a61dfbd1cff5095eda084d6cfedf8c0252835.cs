using System;

using Plossum.CommandLine;

namespace RootNavLinux
{
	[CommandLineManager(ApplicationName = "RootNavLinux", Copyright = "University of Nottingham")]
	public class RootNavOptions
	{
		public RootNavOptions ()
		{
			
		}

		[CommandLineOption(Description = "Displays this help text")]
		public bool Help = false;

		[CommandLineOption(Description = "Specifies the input file", MinOccurs = 0)]
		public string Name
		{
			get { return mName; }
			set
			{
				if (String.IsNullOrEmpty(value))
					throw new InvalidOptionValueException("The name must not be empty", false);
				mName = value;
			}
		}

		private string mName;

		public int InitialClassCount { get; set; }
		public int MaximumClassCount { get; set; }
		public int ExpectedRootClassCount { get; set; }
		public int PatchSize { get; set; }
		public double BackgroundPercentage { get; set; }
		public double BackgroundExcessSigma { get; set; }
		public double[] Weights { get; set; }
		public Object Default { get; set; }
	}


}
