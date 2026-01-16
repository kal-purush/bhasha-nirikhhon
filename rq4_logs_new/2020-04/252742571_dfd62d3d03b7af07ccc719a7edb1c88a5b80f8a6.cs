using Microsoft.Extensions.Configuration;
using System;
using System.Linq;

namespace Snippy.Data
{
	public interface IDataConfiguration
	{
		public string ConnectionString { get; }
	}

	public class DataConfiguration : IDataConfiguration
	{
		public DataConfiguration(IConfiguration Configuration)
		{
			Configuration.Bind("DBConfig", this);
			var section = Configuration.GetSection("DBConfig");
		}

	public string ConnectionString { get; }
	}
}