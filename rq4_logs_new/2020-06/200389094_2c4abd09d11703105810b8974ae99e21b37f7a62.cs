namespace Ontology
{
    public class DataPropertyRange:
        DataPropertyAxiom,
        IDataPropertyRange
    {
        private IDataRange _range;

        public DataPropertyRange(
            IOntology               ontology,
            IDataPropertyExpression dataPropertyExpression,
            IDataRange              range
            ) : base(
                ontology,
                dataPropertyExpression)
        {
            _range = range;
        }

        IDataRange IDataPropertyRange.Range => _range;
    }
}