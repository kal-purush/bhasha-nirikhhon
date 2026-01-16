namespace DuplicateFilesRemover.Test
{
    using System.Collections.Generic;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DirectoryContentRetrieverTest
    {
        private const string TEST_DIRECTORY_PATH = "C:\\Users\\francis beaucage\\Documents\\Visual Studio 2015\\Projects\\DuplicateFilesRemover\\DuplicateFilesRemover.Test\\TestDirectory";

        private readonly List<string> _directoryContent = new List<string> { TEST_DIRECTORY_PATH + "\\TestFile1.txt", TEST_DIRECTORY_PATH + "\\TestFile2.txt" };

        [TestMethod]
        public void CanRetrieveContentOfTestDirectory()
        {
            var retriever = new DirectoryContentRetriever();
            var fileNames = retriever.List(TEST_DIRECTORY_PATH);
            CollectionAssert.AreEqual(_directoryContent, fileNames);
        }
    }
}