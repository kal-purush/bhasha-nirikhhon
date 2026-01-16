using System;
using VDS.RDF;

namespace CT.CASE.Bindings
{
    /// <summary>
    /// A single RDF graph conforming to the CASE ontology schema.
    /// </summary>
    public sealed partial class DataSet
    {
        public Graph Graph { get; private set; }

        private readonly INode A;

        public DataSet()
        {
            Graph = new Graph();
            A = Graph.CreateUriNode("rdf:type");
        }

        private T _Create<T>(Uri identifier, Uri classIri, Func<DataSet, INode, T> constructor)
        {
            var subject = (identifier == null)
                ? (INode)Graph.CreateBlankNode()
                : Graph.CreateUriNode(identifier);
            Graph.Assert(new Triple(subject, A, Graph.CreateUriNode(classIri)));
            return constructor(this, subject);
        }
    }
}