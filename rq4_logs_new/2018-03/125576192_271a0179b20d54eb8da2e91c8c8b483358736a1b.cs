using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nikita.Lib.Data;

namespace Nikita.Lib.UnitTests
{
    [TestClass]
    public class FileParserTest
    {
        private DataMeta dataMeta1;
        private DataMeta dataMeta2;
        private PersonParser recordParser;
        private HeaderParser headerParser;
        private DataFileParser<Person> personFileParser;
        private string testFilePath = @".\\mockData\\PersonTestFile.txt";
        private string testFileEmptyPath = @".\\mockData\\EmptyFile.txt";

        private string badTestFilePath = @".\\mockData\\doesnotexist.txt";

       

        [TestInitialize]
        public void Initialize()
        {
            recordParser = new PersonParser();
            headerParser = new HeaderParser();
            personFileParser = new DataFileParser<Person>(headerParser, recordParser);
    
        }

        [TestMethod]
        public void FileParse_Parse_Test()
        {
            var df = personFileParser.Parse(testFilePath);
            var recordCount = df.Items.Count;

            
            Assert.AreEqual(4,recordCount);

        }

        [TestMethod]
        public void FileParse_Parse_Bad_Test()
        {
            var df = personFileParser.Parse(badTestFilePath);

            Assert.AreEqual(1, df.Errors.Count);

            var error = df.Errors.FirstOrDefault();
            
            Assert.IsInstanceOfType(error.Exception, typeof (IOException)  );
            //
        }

        [TestMethod]
        public void FileParse_Parse_Empty_Test()
        {
            var df = personFileParser.Parse(testFileEmptyPath);

            Assert.IsNotNull(df.Meta.Error); 
            Assert.AreEqual(0, df.Meta.Fields.Count);  
        }
    }    
}