using RDFSharp.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace RDFSharp.UnitTests
{
    public static class TestRDFObjectProvider
    {
        public static Regex blankNodeRegex = new Regex("bnode:.*");
        public static string invalidURIString = "invalid.uri.string";
        public static List<string> validURIStrings = new List<string>(){
            "https://en.wikipedia.org/wiki/Joseph_Haydn",
            "https://en.wikipedia.org/wiki/Elizabeth_II",
            "https://en.wikipedia.org/wiki/Magnus_Carlsen"
        };
        public static List<RDFResource> subjectResources = new List<RDFResource>()
        {
            new RDFResource(validURIStrings[0]),
            new RDFResource(validURIStrings[1]),
            new RDFResource(validURIStrings[2])
        };
        public static List<RDFResource> predicateResources = new List<RDFResource>()
        {
            new RDFResource("http://xmlns.com/foaf/0.1/age"),
            new RDFResource("http://xmlns.com/foaf/0.1/name"),
            new RDFResource("http://xmlns.com/foaf/0.1/birthday")
        };
        public static List<RDFLiteral> objectLiterals = new List<RDFLiteral>()
        {
            new RDFTypedLiteral("95", RDFModelEnums.RDFDatatypes.XSD_INTEGER),
            new RDFPlainLiteral("Joseph Haydn", "en-US"),
            new RDFPlainLiteral("11-30")
        };
        public static List<RDFTriple> triples = new List<RDFTriple>()
        {
            new RDFTriple(subjectResources[0], predicateResources[0], objectLiterals[0]),
            new RDFTriple(subjectResources[1], predicateResources[1], objectLiterals[1]),
            new RDFTriple(subjectResources[2], predicateResources[2], objectLiterals[2])
        };
    }
}