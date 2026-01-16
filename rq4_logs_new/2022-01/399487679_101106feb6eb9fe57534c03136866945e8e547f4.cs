namespace Igtampe.Neco.Common.IDGenerators {

    /// <summary>Generates a numerical ID of any given length</summary>
    public class NumericalGenerator : IDGenerator<string> {

        /// <summary>Length of the numerical ID that's generated</summary>
        private readonly int Length;

        /// <summary>Creates a Numerical Generator</summary>
        /// <param name="Length">Length of IDs generated with this </param>
        public NumericalGenerator(int Length) {
            if (Length < 2) { throw new ArgumentException("Length must be at least 2"); }
            this.Length = Length; 
        }

        /// <summary>Generates an ID</summary>
        /// <returns></returns>
        public override string Generate() {
            Random R = new();

            int ID = R.Next(8) + 1 * Convert.ToInt32(Math.Pow(10, Length - 1));
            ID += R.Next(Convert.ToInt32(Math.Pow(10, Length - 1)));

            return ID.ToString();
        }
    }
}