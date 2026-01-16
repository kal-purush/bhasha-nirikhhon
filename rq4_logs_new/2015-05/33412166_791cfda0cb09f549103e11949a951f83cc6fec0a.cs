using NUnit.Framework;
using HomeAutomationApp.iOS;
using System.IO;
using Foundation;
using OpenEars;

namespace iOSUnitTests
{
[TestFixture]
public class VoiceRecognitionTests
{
	HomeViewRenderer renderer;

	[TestFixtureSetUp]
	public void Init()
	{
		renderer = new HomeViewRenderer();
	}

	[Test]
	public void TestDictionaryFileExistence()
	{

		Assert.True(File.Exists(renderer.pathToDictionary));
	}

	[Test]
	public void TestLanguageModelFileExistence()
	{
		Assert.True(File.Exists(renderer.pathToLanguageModel));
	}

	[Test]
	public void TestModelGeneration()
	{
		string[] names = { "MAKE", "IT", "BRIGHTER", "NEAR", "ME", "" };

		var pathToAcousticModel = NSBundle.MainBundle.ResourcePath + System.IO.Path.DirectorySeparatorChar + "AcousticModelEnglish.bundle";

		var generator = new OELanguageModelGenerator();
		NSError error = generator.GenerateLanguageModelFromArray(NSArray.FromStrings(names), "HomeAutomationLanguageModel", pathToAcousticModel);
	
		Assert.IsNull(error);
	}

	[Test]
	public void TestListening()
	{
		Assert.DoesNotThrow(new TestDelegate(renderer.StartListening));
		renderer.StopListening();
	}

}
}