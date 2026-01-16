using System.Collections.Generic;

namespace OrionSample.Api.Shared
{
    public partial class Decimal
    {
        private const decimal NanoFactor = 1_000_000_000;

        // Example: 12345.6789 -> { units = 12345, nanos = 678900000 }
        public Decimal(long units, int nanos)
        {
            Units = units;
            Nanos = nanos;
        }

        public static implicit operator decimal(Decimal value)
        {
            return value.Units + value.Nanos / NanoFactor;
        }

        public static implicit operator Decimal(decimal value)
        {
            var units = decimal.ToInt64(value);
            var nanos = decimal.ToInt32((value - units) * NanoFactor);
            return new Decimal(units, nanos);
        }
    }
}