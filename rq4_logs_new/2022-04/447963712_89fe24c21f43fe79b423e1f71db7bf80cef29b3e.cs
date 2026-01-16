using System;
using System.Text;
using OSIsoft.Data;

namespace PItoADHReadOnly
{
    public class PItoADHEvent
    {
        [SdsMember(IsKey = true)]
        public DateTime Timestamp { get; set; }

        public float? Value { get; set; }

        public bool IsQuestionable { get; set; }

        public bool IsSubstituted { get; set; }

        public bool IsAnnotated { get; set; }

        public int? SystemStateCode { get; set; }

        public string DigitalStateName { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append($"Timestamp: {Timestamp}, ");

            if (Value is not null)
            {
                sb.Append($"Value: {Value}, ");
            }

            sb.Append($"IsQuestionable: {IsQuestionable}, ");
            sb.Append($"IsSubstituted: {IsSubstituted}, ");
            sb.Append($"IsAnnotated: {IsAnnotated}");

            // In case Value is null, the event will specify a SystemStateCode
            // integer with DigitalStateName as its string representation
            if (SystemStateCode is not null)
            {
                sb.Append($", SystemStateCode: {SystemStateCode}, ");
                sb.Append($"DigitalStateName: {DigitalStateName}");
            }

            return sb.ToString();
        }
    }
}