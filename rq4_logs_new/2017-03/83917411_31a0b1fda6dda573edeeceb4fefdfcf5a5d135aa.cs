namespace saaaaaa
{
	using SourceAFIS.Simple;
	using System;
	using System.Drawing;
	using System.IO;
	using System.Linq;
	using System.Threading;

	class MainClass
	{
		static AfisEngine Afis = new AfisEngine();
		public static void Main(string[] args)
		{
			Console.WriteLine("Hello World!");

			//Database Start

			Fingerprint fp1 = new Fingerprint();
			fp1.AsBitmap = new Bitmap(Bitmap.FromFile("C:\\Users\\Jason\\Documents\\Projects\\saaaaaa\\saaaaaa\\Database\\2L.bmp"));
			Person person1 = new Person();
			person1.Fingerprints.Add(fp1);
			Afis.Extract(person1);

			Fingerprint fp2 = new Fingerprint();
			fp2.AsBitmap = new Bitmap(Bitmap.FromFile("C:\\Users\\Jason\\Documents\\Projects\\saaaaaa\\saaaaaa\\Database\\3L.bmp"));
			Person person2 = new Person();
			person2.Fingerprints.Add(fp2);
			Afis.Extract(person2);

			Fingerprint fp3 = new Fingerprint();
			fp3.AsBitmap = new Bitmap(Bitmap.FromFile("C:\\Users\\Jason\\Documents\\Projects\\saaaaaa\\saaaaaa\\Database\\4L.bmp"));
			Person person3 = new Person();
			person3.Fingerprints.Add(fp3);
			Afis.Extract(person3);

			//Database End

			//Testing
			//awaiting finger input
			int i = 0; 

			while (true)
			{
				if(Directory.GetFiles("C:\\Images", "*.bmp").ToArray().Length != i)
				{
					//Directory.EnumerateFileSystemEntries("C:\\Images").Any()
					i = i + 1;

					Fingerprint fp4 = new Fingerprint();

					string[] bmpFiles = Directory.GetFiles("C:\\Images", "*.bmp").ToArray();
					string filename = bmpFiles[i-1];
					fp4.AsBitmap = new Bitmap(Bitmap.FromFile(filename));

					Person test = new Person();
					test.Fingerprints.Add(fp4);
					Afis.Extract(test);

					float score1 = Afis.Verify(test, person1);
					float score2 = Afis.Verify(test, person2);
					float score3 = Afis.Verify(test, person3);

					bool match1 = (score1 > 0);
					bool match2 = (score2 > 0);
					bool match3 = (score3 > 0);

					//Console.WriteLine(match);
					Console.WriteLine(match1);
					Console.WriteLine(match2);
					Console.WriteLine(match3);

					if (match1)
					{
						//911
						Console.WriteLine("911");
					}
					else if (match2)
					{
						//IoT
						Console.WriteLine("IoT");
					}
					else if (match3)
					{
						//TextMsg
						Console.WriteLine("912");
					}

					Directory.Delete("C:\\Images\\bvs");
				}
			}
		}
	}
}