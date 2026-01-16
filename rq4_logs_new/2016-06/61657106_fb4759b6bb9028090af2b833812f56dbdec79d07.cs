namespace Bands.Validation
{
    /// <summary>
    /// Defines an object capable of validating another object of type <typeparamref name="TValidatable"/>
    /// </summary>
    /// <typeparam name="TValidatable">Type of validatable object</typeparam>
    public interface IValidator<TValidatable> where TValidatable : IValidatable
    {
        /// <summary>
        /// Validates object <paramref name="validatable"/>
        /// </summary>
        /// <param name="validatable">A validatable object of type <typeparamref name="TValidatable"/></param>
        /// <returns>True if <param name="validatable" /> is valid</returns>
        bool Validate(TValidatable validatable);
    }
}