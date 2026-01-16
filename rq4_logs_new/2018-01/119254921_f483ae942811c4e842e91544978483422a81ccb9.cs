using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace CompTech.Ict.Sample.Models
{
    //public class ComputationGraph
    //{
	    //List<int> Finished_command;
		public class Comp_Graph
		{
			[JsonProperty("depend")]
			public int[,] Dependecies { get; set; }
			[JsonProperty("operations")]
			public List<Operation> Operations { get; set; }
			[JsonProperty("mnemonics_values")]
			public List<MnemonicsValue> MnemonicsValues { get; set; }
		}
		public class Operation
		{
			[JsonProperty("id")]
			public int Id { get; set; }
			[JsonProperty("name")]
			public string Name { get; set; }
			[JsonProperty("input")]
			public string [] Input { get; set; }
			[JsonProperty("output")]
			public string [] Output { get; set; }
			[JsonProperty("parameters")]
			public List<DataType> Parameters { get; set; }

		}
		public class DataType
		{
			[JsonProperty("name")]
			public string Name { get; set; }
			//parameters??
		}
		public class MnemonicsValue
		{
			[JsonProperty("value")]
			public string Value { get; set; }
			[JsonProperty("type")]
			public List<DataType> Type { get; set; }
		}
	
	//}
	
}