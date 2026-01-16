namespace Test.AzurePipelines
{
    /// <summary>
    /// An abstraction.
    /// </summary>
    public interface IInterface1
    {
        /// <summary>
        /// Converts all of the characters in a string to lowercase.
        /// </summary>
        string ToLower(string message);

        /// <summary>
        /// Converts all of the characters in a string to uppercase.
        /// </summary>
        string ToUpper(string message);
    }
}