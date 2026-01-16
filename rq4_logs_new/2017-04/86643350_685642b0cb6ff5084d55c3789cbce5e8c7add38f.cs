namespace OutcoldSolutions.ConfigTransformationTool.Suites
{
    public static class StringExtensions
    {
        /// <summary>
        /// Returns a new string whose textual value is similar to this string, but line endings are in Unix-style (\n).
        /// </summary>
        public static string NormalizeNewLine(this string s)
        {
            return s.Replace("\r\n", "\n");
        }
    }
}