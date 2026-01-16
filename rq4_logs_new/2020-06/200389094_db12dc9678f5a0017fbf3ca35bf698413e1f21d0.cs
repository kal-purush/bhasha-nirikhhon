namespace Ontology
{
    public abstract class DataPropertyAxiom:
        Axiom,
        IDataPropertyAxiom
    {
        private IDataPropertyExpression _dataPropertyExpression;

        protected DataPropertyAxiom(
            IOntology               ontology,
            IDataPropertyExpression dataPropertyExpression
            ) : base(ontology)
        {
            _dataPropertyExpression = dataPropertyExpression;
        }

        IDataPropertyExpression IDataPropertyAxiom.DataPropertyExpression => _dataPropertyExpression;
    }
}