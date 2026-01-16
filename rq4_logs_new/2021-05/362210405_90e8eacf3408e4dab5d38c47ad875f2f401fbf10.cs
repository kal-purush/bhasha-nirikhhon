using System;
using System.Data;
using System.IO;
using RDFSharp;
using RDFSharp.Model;
using RDFSharp.Query;

namespace NonFunctionalTests {
    class Program {
        static void Main(string[] args) {

            // load file to graph
            Console.WriteLine("Loading file...");

            FileStream fs = File.OpenRead(@"..\..\..\test_data\szepmuveszeti.n3");
            var museum_graph = RDFGraph.FromStream(RDFModelEnums.RDFFormats.Turtle, fs);
            Console.WriteLine($"Triples count: {museum_graph.TriplesCount,0:N0}");

            // calc total and avarge uri length
            long sum_uri_len = 0;
            foreach (var triple in museum_graph) {
                sum_uri_len += triple.Subject.ToString().Length;
                sum_uri_len += triple.Predicate.ToString().Length;
                sum_uri_len += triple.Object.ToString().Length;
            }
            Console.WriteLine($"Sum. uri len: {sum_uri_len,0:N0}\nAvg. uri len: {sum_uri_len / (museum_graph.TriplesCount * 3)}");

            // select query
            var ns_ecrm = new RDFNamespace("ecrm", " http://erlangen-crm.org/current/");
            RDFVariable actor = new RDFVariable("actor");
            RDFVariable note = new RDFVariable("note");
            RDFVariable label = new RDFVariable("label");

            RDFSelectQuery query = new RDFSelectQuery()
                .AddPrefix(RDFNamespaceRegister.GetByPrefix("ecrm"))
                .AddPatternGroup(new RDFPatternGroup("PG1")
                    .AddPattern(new RDFPattern(actor, RDFVocabulary.RDF.TYPE, new RDFResource(ns_ecrm + "E39_Actor")))
                    .AddPattern(new RDFPattern(actor, new RDFResource(ns_ecrm + "P3_has_note"), note))
                    .AddPattern(new RDFPattern(actor, RDFVocabulary.RDFS.LABEL, label)))
                .AddProjectionVariable(actor)
                .AddProjectionVariable(label);

            Console.WriteLine("\nQuerry in progress");
            RDFSelectQueryResult result = query.ApplyToGraph(museum_graph);
            Console.WriteLine("Results count: " + result.SelectResultsCount);

            //print results
            foreach (DataRow row in result.SelectResults.Rows) {
                foreach (var item in row.ItemArray) {
                    Console.Write(item + ", ");
                }
                Console.WriteLine();
            }

        }
    }
}