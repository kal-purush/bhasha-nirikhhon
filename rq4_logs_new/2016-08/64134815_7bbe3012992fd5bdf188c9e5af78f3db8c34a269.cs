using HTTPServer.core;

namespace HTTPServer.test
{
    public class MockDirectoryContents: IDirectoryContents
    {
        public string DirectoryPath { get; }

        public MockDirectoryContents(string directoryPath)
        {
            DirectoryPath = directoryPath;
        }
        public string[] GetFiles()
        {
            return new []{ "C:\\gitwork\\HTTP Server\\.gitattributes" , "C:\\gitwork\\HTTP Server\\.gitignore" , "C:\\gitwork\\HTTP Server\\HTTP Server.core.dll" , "C:\\gitwork\\HTTP Server\\HTTPServer.sln" , "C:\\gitwork\\HTTP Server\\server.config" , "C:\\gitwork\\HTTP Server\\server.exe" };
        }

        public string[] GetDirectories()
        {
            return new []{ "C:\\gitwork\\HTTP Server\\.git" , "C:\\gitwork\\HTTP Server\\.vs" , "C:\\gitwork\\HTTP Server\\HTTP Server.core" , "C:\\gitwork\\HTTP Server\\HTTPServer.driver" , "C:\\gitwork\\HTTP Server\\HTTPServer.run" , "C:\\gitwork\\HTTP Server\\HTTPServer.test" , "C:\\gitwork\\HTTP Server\\packages" , "C:\\gitwork\\HTTP Server\\TestResults" };
        }
    }
}