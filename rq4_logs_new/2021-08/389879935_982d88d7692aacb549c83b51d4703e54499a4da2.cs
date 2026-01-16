namespace CarsInfo.DAL.Assistance
{
    public class FilterModel
    {
        public FilterModel(
            string field,
            int value,
            string @operator = "=",
            string separator = "")
        {
            Field = field;
            Value = value;
            Operator = @operator;
            Separator = separator;
        }

        public FilterModel(
            string field,
            object value,
            string @operator = "=",
            string separator = "")
        {
            Field = field;
            Value = value;
            Operator = @operator;
            Separator = separator;
        }

        public string Field { get; set; }

        public object Value { get; set; }

        public string Operator { get; set; }

        public string Separator { get; set; }
    }
}