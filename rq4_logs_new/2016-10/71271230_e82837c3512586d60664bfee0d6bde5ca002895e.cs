using Dusyk.DeploymentScriptCreator.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dusyk.DeploymentScriptCreator.Oracle
{
	public class OraclePackageCreator : IPackageCreator
	{
		public List<string> Files { get; set; }

		public string OutputPath { get; set; }

		public string OutputFileName { get; set; }

		public bool CreateScript()
		{
			try
			{

			}
			catch (Exception ex)
			{
				// log ex
				return false;
			}

			return true;
		}
	}
}