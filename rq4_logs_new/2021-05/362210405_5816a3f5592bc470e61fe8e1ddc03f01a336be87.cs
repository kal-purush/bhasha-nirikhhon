using Microsoft.VisualStudio.TestTools.UnitTesting;
using RDFSharp.Query;
using RDFSharp.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace RDFSharp.UnitTests
{
    [TestClass]
    public class RDFSelectQueryTest
    {
        private static RDFGraph graph = TestRDFObjectProvider.graph;
        private static string projectPath = Directory.GetParent(Environment.CurrentDirectory).Parent.Parent.FullName;
        private static string emptyQueryResultPath = Path.Combine(projectPath, "EmptyQueryResult.xml");
        private static string emptyQueryTestResultPath = "EmptyQueryTestResult.xml";
        private static string nonEmptyQueryResultPath = Path.Combine(projectPath, "NonEmptyQueryResult.xml");
        private static string nonEmptyQueryTestResultPath = "NonEmptyQueryTestResult.xml";

        private static string selectEverythingString = "SELECT *\n";
        private static string foafPrefixString = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n\n";
        private static RDFNamespace foafPrefix = RDFNamespaceRegister.GetByPrefix("foaf");
        private static string selectString(List<string> variables)
        {
            var selectString = "SELECT";
            foreach (var v in variables)
            {
                selectString += " ?" + v;
            }
            selectString += "\n";
            return selectString;
        }
        private static string whereString(List<string> patternGroups = null)
        {
            var whereString = "WHERE {";
            if (patternGroups != null)
            {
                foreach (var pg in patternGroups)
                {
                    whereString += "\n" + pg;
                }
            }
            else
                whereString += "\n";
            whereString += "}\n";
            return whereString;
        }
        private static string patternGroupString(List<string> patterns)
        {
            var patternGroupString = "  {";
            foreach (var p in patterns)
            {
                patternGroupString += "\n    " + p + " .";
            }
            patternGroupString += "\n  }\n";
            return patternGroupString;
        }
        private static string patternString(string subject, string predicate, string obj)
        {
            return "?" + subject + " " + predicate + " ?" + obj;
        }


        [TestMethod]
        public void RDFSelectQuery_DefaultCtor_ReturnsGeneralSelectQuery()
        {
            //Arrange
            var desiredResult = selectEverythingString + whereString();

            //Act
            var query = new RDFSelectQuery();
            var result = query.ToString();

            //Assert
            Assert.AreEqual(desiredResult, result);
        }

        [TestMethod]
        public void AddPrefix_GeneralSelectQuery_ReturnsGeneralSelectQueryWithPrefix()
        {
            //Arrange
            var query = new RDFSelectQuery();
            var desiredResult = foafPrefixString
                                + selectEverythingString
                                + whereString();

            //Act
            query.AddPrefix(foafPrefix);
            var result = query.ToString();

            //Assert
            Assert.AreEqual(desiredResult, result);
        }

        [TestMethod]
        public void AddProjectionVariables_DefaultQueryWithoutWhere_ReturnsSelectQueryWithProjection()
        {
            //Arrange
            var query = new RDFSelectQuery();
            query.AddPrefix(foafPrefix);
            RDFVariable x = new RDFVariable("X");
            RDFVariable y = new RDFVariable("Y");
            var desiredResult = foafPrefixString
                                + selectString(new List<string>() { "X", "Y"})
                                + whereString();

            //Act
            query.AddProjectionVariable(x)
                .AddProjectionVariable(y);
            var result = query.ToString();

            //Assert
            Assert.AreEqual(desiredResult, result);
        }

        [TestMethod]
        public void AddPatternGroup_QueryWithProjectionWithoutWhere_ReturnsSelectQueryWithWhere()
        {
            //Arrange
            var query = new RDFSelectQuery();
            query.AddPrefix(foafPrefix);
            RDFVariable x = new RDFVariable("X");
            RDFVariable y = new RDFVariable("Y");
            query.AddProjectionVariable(x)
                .AddProjectionVariable(y);

            var patternGroup = new List<string> { patternString("X", "foaf:age", "Y") };
            var patternGroups = new List<string> { patternGroupString(patternGroup) };
            var desiredResult = foafPrefixString
                                + selectString(new List<string>() { "X", "Y" })
                                + whereString(patternGroups);

            //Act
            query.AddPatternGroup(new RDFPatternGroup("PG1")
                .AddPattern(new RDFPattern(x, TestRDFObjectProvider.predicateResources[0], y)));
            var result = query.ToString();

            //Assert
            Assert.AreEqual(desiredResult, result);
        }

        private string getXML(string path)
        {
            //var projectPath = Directory.GetParent(Environment.CurrentDirectory).Parent.Parent.FullName;
            //var fullPath = Path.Combine(projectPath, path);

            var lines = System.IO.File.ReadAllLines(path);
            var xmlString = string.Concat(lines);

            return xmlString;
        }

        [TestMethod]
        public void ApplyToGraph_EmptyQuery_ReturnsEmptySPARQLResult()
        {
            //Arrange
            var query = new RDFSelectQuery();
            var desired = getXML(emptyQueryResultPath);

            //Act
            var results = query.ApplyToGraph(graph);
            results.ToSparqlXmlResult(emptyQueryTestResultPath);

            //Assert
            var actual = getXML(emptyQueryTestResultPath);
            Assert.AreEqual(desired, actual);
        }

        [TestMethod]
        public void ApplyToGraph_NonEmptyQuery_ReturnsSPARQLResult()
        {
            //Arrange
            RDFVariable x = new RDFVariable("X");
            RDFVariable y = new RDFVariable("Y");
            var query = new RDFSelectQuery()
                .AddPrefix(foafPrefix)
                .AddProjectionVariable(x)
                .AddProjectionVariable(y)
                .AddPatternGroup(new RDFPatternGroup("PG1")
                    .AddPattern(new RDFPattern(x, TestRDFObjectProvider.predicateResources[0], y)));
            var desired = getXML(nonEmptyQueryResultPath);

            //Act
            var results = query.ApplyToGraph(graph);
            results.ToSparqlXmlResult(nonEmptyQueryTestResultPath);

            //Assert
            var actual = getXML(nonEmptyQueryTestResultPath);
            Assert.AreEqual(desired, actual);
        }
    }
}