using System;
using System.IO;

namespace RequiredPackagesScriptGen
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			Console.WriteLine ("Generating a script file...");

			string filename = "requirements.txt";
			string scriptFile = "installPacks.sh";

			// Open the file into a StreamReader
			StreamReader file = File.OpenText(filename);
			// Hook a write to the text file.
			StreamWriter writer = new StreamWriter(scriptFile);

			// Read the file into a string
			//string s = file.ReadToEnd();

			writer.WriteLine ("sudo apt-get install python-dev");
			writer.WriteLine ("sudo apt-get install python-virtualenv");
			writer.WriteLine ("sudo apt-get install python-dev");

			writer.WriteLine ("");

			while (!file.EndOfStream) 
			{
				string line = file.ReadLine ();	

				if (line.Length > 0) {
					if (line [0] != '#' && line [0] != '-') {
						// Add a single line
						string newPackage = "sudo pip install " + line;
						writer.WriteLine (newPackage);	
					}
				}

			}

			//write special packages
			writer.WriteLine ("sudo pip install Minimatic-1.0.4-py2-none-any.whl");
			writer.WriteLine ("sudo pip install Paste-1.7.5.2-py2-none-any.whl");
			writer.WriteLine ("sudo pip install PasteScript-1.7.3-py2-none-any.whl");
			writer.WriteLine ("sudo pip install TurboGears2-2.1.5-py2-none-any.whl");
			writer.WriteLine ("sudo pip install httplib2-0.7.1_1-py2-none-any.whl");
			writer.WriteLine ("sudo pip install numpy-1.11.0-cp27-cp27mu-linux_x86_64.whl");
			writer.WriteLine ("sudo pip install tgext.registration2-0.5.2-py2-none-any.whl");
			writer.WriteLine ("sudo pip install tw.output-0.5.0dev_20110906-py2-none-any.whl");
			writer.WriteLine ("sudo pip install pylibtiff-0.3.0_1-cp27-none-linux_x86_64.whl");

			// Close the file so it can be accessed again.
			file.Close();
			// Close the writer
			writer.Close();

			Console.WriteLine ("Finished!");
		}
	}
}