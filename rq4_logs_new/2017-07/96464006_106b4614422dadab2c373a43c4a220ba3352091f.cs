namespace AgileTools.Analysers
{
    /// <summary>
    /// Tells is a the result from an analyser can be transformed to a specific format
    /// </summary>
    public interface ITransformableResult
    {
        object Transform(string format, string destinationFormat);
        bool CanTransform(string format, string destinationFormat);
    }
}