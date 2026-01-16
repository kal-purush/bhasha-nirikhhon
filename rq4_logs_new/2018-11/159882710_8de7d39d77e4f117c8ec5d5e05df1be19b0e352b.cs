using System;
using pawrser.classes;

namespace pawrser {

	class Program {
		static void Main(string[] args) {
			if (args.Length == 0 || args.Length > 2) {
				PrintUsage();
				return;
			}
			var source = args[0];
			var destination = args.Length == 2 ? args[1] : null;

			var srcFileConfig = new SrcFileConfig(source);
			var dbFileConfig = new DbFileConfig(destination);

			Console.WriteLine(srcFileConfig.GetFilePath());
			Console.WriteLine(dbFileConfig.GetFilePath());
		}

		static void PrintUsage() {
			Console.WriteLine("Usage: dotnet AnimeTreePawrser.dll LIB_2018_04_21.txt [result.sqlite]");
		}
	}
}